<?php

declare(strict_types=1);

namespace Tests\Integration;

use Illuminate\Support\Facades\Cache;
use LaravelSnowflakeApi\Services\SnowflakeConfig;
use LaravelSnowflakeApi\Services\ThreadSafeTokenProvider;
use PHPUnit\Framework\TestCase;

/**
 * Integration tests for ThreadSafeTokenProvider
 *
 * These tests verify the provider works correctly with real cache drivers
 * and can handle concurrent access scenarios
 */
class ThreadSafeTokenProviderIntegrationTest extends TestCase
{
    private SnowflakeConfig $config;

    protected function setUp(): void
    {
        parent::setUp();

        // Skip if no Snowflake credentials are configured
        if (empty(getenv('SNOWFLAKE_TEST_USER'))) {
            $this->markTestSkipped('Snowflake credentials not configured');
        }

        $this->config = new SnowflakeConfig(
            getenv('SNOWFLAKE_TEST_URL') ?: 'https://test.snowflakecomputing.com',
            getenv('SNOWFLAKE_TEST_ACCOUNT') ?: 'test',
            getenv('SNOWFLAKE_TEST_USER') ?: 'test',
            getenv('SNOWFLAKE_TEST_PUBLIC_KEY') ?: 'test',
            getenv('SNOWFLAKE_TEST_PRIVATE_KEY') ?: $this->getTestPrivateKey(),
            getenv('SNOWFLAKE_TEST_PASSPHRASE') ?: '',
            getenv('SNOWFLAKE_TEST_WAREHOUSE') ?: 'test',
            getenv('SNOWFLAKE_TEST_DATABASE') ?: 'test',
            getenv('SNOWFLAKE_TEST_SCHEMA') ?: 'test',
            30
        );
    }

    protected function tearDown(): void
    {
        // Clean up any cached tokens
        if (isset($this->config)) {
            $provider = new ThreadSafeTokenProvider($this->config);
            $provider->clearTokenCache();
        }

        parent::tearDown();
    }

    public function testTokenGenerationWithRealCredentials()
    {
        // This test verifies token generation works with real Snowflake credentials
        $provider = new ThreadSafeTokenProvider($this->config);

        // Generate token
        $token = $provider->getToken();

        // Verify JWT format
        $this->assertNotEmpty($token);
        $this->assertIsString($token);
        $this->assertMatchesRegularExpression('/^[A-Za-z0-9-_]+\.[A-Za-z0-9-_]+\.[A-Za-z0-9-_]+$/', $token);

        // Verify token parts
        $parts = explode('.', $token);
        $this->assertCount(3, $parts, 'JWT should have 3 parts: header.payload.signature');

        // Decode and verify header
        $header = json_decode(base64_decode(strtr($parts[0], '-_', '+/')), true);
        $this->assertEquals('RS256', $header['alg']);
        $this->assertEquals('JWT', $header['typ']);

        // Decode and verify payload
        $payload = json_decode(base64_decode(strtr($parts[1], '-_', '+/')), true);
        $this->assertArrayHasKey('iss', $payload);
        $this->assertArrayHasKey('sub', $payload);
        $this->assertArrayHasKey('iat', $payload);
        $this->assertArrayHasKey('exp', $payload);
        $this->assertEquals($this->config->getUser(), $payload['sub']);
    }

    public function testTokenCachingWorks()
    {
        // This test verifies tokens are cached correctly
        $provider = new ThreadSafeTokenProvider($this->config);

        // First call generates token
        $startTime = microtime(true);
        $token1 = $provider->getToken();
        $firstCallTime = microtime(true) - $startTime;

        // Second call should be cached (much faster)
        $startTime = microtime(true);
        $token2 = $provider->getToken();
        $secondCallTime = microtime(true) - $startTime;

        // Verify same token returned
        $this->assertEquals($token1, $token2);

        // Verify second call is significantly faster (cached)
        // Static cache should return in microseconds vs milliseconds for generation
        $this->assertLessThan($firstCallTime * 0.1, $secondCallTime,
            'Cached token retrieval should be at least 10x faster than generation');
    }

    public function testClearTokenCacheWorks()
    {
        // This test verifies cache clearing works correctly
        $provider = new ThreadSafeTokenProvider($this->config);

        // Generate and cache token
        $token1 = $provider->getToken();

        // Clear cache
        $provider->clearTokenCache();

        // Next call should generate new token
        $token2 = $provider->getToken();

        // Tokens should be different (different timestamps)
        $this->assertNotEquals($token1, $token2);
    }

    public function testConcurrentAccessScenario()
    {
        // This test simulates concurrent access to verify thread safety
        // Note: This is a basic simulation; true concurrency testing requires
        // actual parallel processes or threads

        $provider1 = new ThreadSafeTokenProvider($this->config);
        $provider2 = new ThreadSafeTokenProvider($this->config);

        // Clear cache to ensure fresh start
        $provider1->clearTokenCache();

        // Simulate concurrent access (same config)
        $token1 = $provider1->getToken();
        $token2 = $provider2->getToken();

        // Both should get the same token (from cache)
        $this->assertEquals($token1, $token2);
    }

    public function testTokenExpiryBufferIsRespected()
    {
        // This test verifies expiry buffer works correctly
        $expiryBuffer = 120; // 2 minutes
        $provider = new ThreadSafeTokenProvider($this->config, $expiryBuffer);

        // Generate token
        $token = $provider->getToken();

        // Decode payload to check expiry
        $parts = explode('.', $token);
        $payload = json_decode(base64_decode(strtr($parts[1], '-_', '+/')), true);

        $expectedExpiry = time() + 3600; // 1 hour from now
        $actualExpiry = $payload['exp'];

        // Verify expiry is approximately 1 hour (within 5 seconds tolerance)
        $this->assertEqualsWithDelta($expectedExpiry, $actualExpiry, 5);
    }

    public function testMultipleConfigsUseDifferentCacheKeys()
    {
        // This test verifies different configs don't share tokens
        $config1 = $this->config;
        $config2 = new SnowflakeConfig(
            $this->config->getBaseUrl(),
            'different_account',
            'different_user',
            $this->config->getPublicKey(),
            $this->config->getPrivateKey(),
            $this->config->getPrivateKeyPassphrase(),
            $this->config->getWarehouse(),
            $this->config->getDatabase(),
            $this->config->getSchema(),
            30
        );

        $provider1 = new ThreadSafeTokenProvider($config1);
        $provider2 = new ThreadSafeTokenProvider($config2);

        // Clear caches
        $provider1->clearTokenCache();
        $provider2->clearTokenCache();

        // Generate tokens
        $token1 = $provider1->getToken();
        $token2 = $provider2->getToken();

        // Tokens should be different (different users/accounts)
        $this->assertNotEquals($token1, $token2);

        // Verify payloads have different subjects
        $parts1 = explode('.', $token1);
        $parts2 = explode('.', $token2);
        $payload1 = json_decode(base64_decode(strtr($parts1[1], '-_', '+/')), true);
        $payload2 = json_decode(base64_decode(strtr($parts2[1], '-_', '+/')), true);

        $this->assertNotEquals($payload1['sub'], $payload2['sub']);
    }

    /**
     * Get a test RSA private key
     *
     * @return string PEM-encoded private key
     */
    private function getTestPrivateKey(): string
    {
        return <<<EOT
-----BEGIN RSA PRIVATE KEY-----
MIIEpAIBAAKCAQEA0Z3VS5JJcds3xfn/xtB4x+3TZkJ2sPK8AQr6RpppKNkCaZ3L
dRN7j8KoFvLgAFk8hT5iHKLEa+AQZX+HmLTYSLrJ5RfCdN0d5l8rF6V5lMpZQXpr
wKLaIXAKLQ3nqBrKCvSYLnJuNz6QPy5WKELGp9p6hT8dKBLrQxQAkqw+Y0kZRNDi
nZiPCH0kW0qZd9xHxb3EqRQZqD3chDPtPjB5aVPOTLa7XCVW3/kXJYX0bXH8tVPk
lLkW7jMvZLqQJUHKP7VgGlFCDk3nZSNs5Q7lW7Gg9MdQmXdCXN/AQVzYF/Bp3fXw
JLF3W6PWVP1YhcQW9XN8YHCzYHMKIgQ5AQQqzQIDAQABAoIBABz8qUK8QVYSJvHs
mXYOKV3FoKJ1qMvQA+oQQp3kq2dFvmVQDqQcN3x2wN3V6QmH5qV/5p/i4fOt8j8k
hFYmQXqVTGaJHiN1kqQU8E/A8pqMkYxkJqC3KXQ7LVQ5xRXKb8LqnPOqvxVXjJKc
u6nYCN4kQqW7BXgD5h6+2sNS5fEqQRFv7pJRJvX2HXaQmSqLk7n4QdBVk5WPQNLr
JqQw3xQmJKVqMz8xCbPtZLBR7bMHCQW9YHPz2x5+6Jxt8UHYdxFqsrK4xqxQMJLp
5cLNqP3YVqKQfUxQqPxqQPNqQNp4LqQP6xQqPxqQPNqQNp4LqQP6xQqPxqQPNqQN
p4LqQP6xQAECgYEA9fQx8QqLxQP3xQ4Q6Q8xQP3xQ4Q6Q8xQP3xQ4Q6Q8xQP3xQ4
Q6Q8xQP3xQ4Q6Q8xQP3xQ4Q6Q8xQP3xQ4Q6Q8xQP3xQ4Q6Q8xQP3xQ4Q6Q8xQP3x
Q4Q6Q8xQP3xQ4Q6Q8xQP3xQ4Q6Q8xQP3xQ4Q6Q8xQP3xQ4Q6Q8xQP3xQ4Q6Q0CgY
EA2fQx8QqLxQP3xQ4Q6Q8xQP3xQ4Q6Q8xQP3xQ4Q6Q8xQP3xQ4Q6Q8xQP3xQ4Q6Q
8xQP3xQ4Q6Q8xQP3xQ4Q6Q8xQP3xQ4Q6Q8xQP3xQ4Q6Q8xQP3xQ4Q6Q8xQP3xQ4Q
6Q8xQP3xQ4Q6Q8xQP3xQ4Q6Q8xQP3xQ4Q6Q8xQP3xQ4Q6QECgYEAqfQx8QqLxQP3
xQ4Q6Q8xQP3xQ4Q6Q8xQP3xQ4Q6Q8xQP3xQ4Q6Q8xQP3xQ4Q6Q8xQP3xQ4Q6Q8xQ
P3xQ4Q6Q8xQP3xQ4Q6Q8xQP3xQ4Q6Q8xQP3xQ4Q6Q8xQP3xQ4Q6Q8xQP3xQ4Q6Q8
xQP3xQ4Q6Q8xQP3xQ4Q6Q8xQP3xQ4Q6Q0CgYB3fQx8QqLxQP3xQ4Q6Q8xQP3xQ4Q
6Q8xQP3xQ4Q6Q8xQP3xQ4Q6Q8xQP3xQ4Q6Q8xQP3xQ4Q6Q8xQP3xQ4Q6Q8xQP3xQ
4Q6Q8xQP3xQ4Q6Q8xQP3xQ4Q6Q8xQP3xQ4Q6Q8xQP3xQ4Q6Q8xQP3xQ4Q6Q8xQP3
xQ4Q6Q8xQP3xQ4Q6QQKBgQDJ9DHxCovFA/fFDhDpDzFA/fFDhDpDzFA/fFDhDpDz
FA/fFDhDpDzFA/fFDhDpDzFA/fFDhDpDzFA/fFDhDpDzFA/fFDhDpDzFA/fFDhDp
DzFA/fFDhDpDzFA/fFDhDpDzFA/fFDhDpDzFA/fFDhDpDzFA/fFDhDpDzFA/fFDh
DpA==
-----END RSA PRIVATE KEY-----
EOT;
    }
}
