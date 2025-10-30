<?php

declare(strict_types=1);

namespace Tests\Unit\Services;

use Exception;
use Illuminate\Contracts\Cache\LockTimeoutException;
use Illuminate\Support\Facades\Cache;
use LaravelSnowflakeApi\Exceptions\SnowflakeApiException;
use LaravelSnowflakeApi\Services\SnowflakeConfig;
use LaravelSnowflakeApi\Services\ThreadSafeTokenProvider;
use Mockery;
use PHPUnit\Framework\TestCase;

class ThreadSafeTokenProviderTest extends TestCase
{
    private SnowflakeConfig $config;

    protected function setUp(): void
    {
        parent::setUp();

        // Create mock config with test credentials
        $this->config = new SnowflakeConfig(
            'https://test.snowflakecomputing.com',
            'test_account',
            'test_user',
            'test_public_key_fingerprint',
            $this->getTestPrivateKey(),
            '',
            'test_warehouse',
            'test_database',
            'test_schema',
            30
        );
    }

    protected function tearDown(): void
    {
        Mockery::close();
        parent::tearDown();
    }

    public function testGetTokenFromStaticCache()
    {
        // This test verifies that static cache is checked first
        $provider = new ThreadSafeTokenProvider($this->config);

        // Mock Laravel cache to ensure it's not called
        Cache::shouldReceive('get')->never();
        Cache::shouldReceive('lock')->never();

        // Pre-populate static cache using reflection
        $reflection = new \ReflectionClass($provider);
        $staticCache = $reflection->getProperty('staticCache');
        $staticCache->setAccessible(true);

        $staticKey = 'test_account:test_user';
        $testToken = 'static_cached_token.test.signature';
        $staticCache->setValue([
            $staticKey => [
                'token' => $testToken,
                'expiry' => time() + 3600, // Valid for 1 hour
            ],
        ]);

        // Get token - should return static cached value
        $token = $provider->getToken();

        $this->assertEquals($testToken, $token);
    }

    public function testGetTokenFromLaravelCache()
    {
        // This test verifies Laravel cache is checked when static cache misses
        $provider = new ThreadSafeTokenProvider($this->config);

        $cacheKey = 'snowflake_api_token:test_account:test_user';
        $testToken = 'laravel_cached_token.test.signature';
        $cachedData = [
            'token' => $testToken,
            'expiry' => time() + 3600,
        ];

        // Mock cache to return token
        Cache::shouldReceive('get')
            ->once()
            ->with($cacheKey)
            ->andReturn($cachedData);

        // Get token - should return Laravel cached value
        $token = $provider->getToken();

        $this->assertEquals($testToken, $token);
    }

    public function testGetTokenGeneratesNewWhenCacheMisses()
    {
        // This test verifies new token generation when all caches miss
        $provider = new ThreadSafeTokenProvider($this->config);

        $cacheKey = 'snowflake_api_token:test_account:test_user';
        $lockKey = 'snowflake_api_token_lock:test_account:test_user';

        // Mock cache miss
        Cache::shouldReceive('get')
            ->once()
            ->with($cacheKey)
            ->andReturn(null);

        // Mock lock acquisition (simulate lock not supported)
        $mockLock = Mockery::mock(\Illuminate\Contracts\Cache\Lock::class);
        Cache::shouldReceive('lock')
            ->once()
            ->with($lockKey, 5)
            ->andReturn($mockLock);

        $mockLock->shouldReceive('block')
            ->once()
            ->andThrow(new Exception('Lock not supported'));

        // Mock cache operations for storing new token
        Cache::shouldReceive('get')
            ->with($cacheKey)
            ->andReturn(null);

        Cache::shouldReceive('put')
            ->once()
            ->withArgs(function ($key, $data, $duration) use ($cacheKey) {
                return $key === $cacheKey
                    && is_array($data)
                    && isset($data['token'])
                    && isset($data['expiry'])
                    && $duration > 0;
            });

        // Get token - should generate new one
        $token = $provider->getToken();

        $this->assertNotEmpty($token);
        $this->assertStringContainsString('.', $token); // JWT format
    }

    public function testLockTimeoutFallsBackToGeneration()
    {
        // This test verifies graceful fallback when lock times out
        $provider = new ThreadSafeTokenProvider($this->config);

        $cacheKey = 'snowflake_api_token:test_account:test_user';
        $lockKey = 'snowflake_api_token_lock:test_account:test_user';

        // Mock cache miss
        Cache::shouldReceive('get')
            ->with($cacheKey)
            ->andReturn(null);

        // Mock lock timeout
        $mockLock = Mockery::mock(\Illuminate\Contracts\Cache\Lock::class);
        Cache::shouldReceive('lock')
            ->once()
            ->with($lockKey, 5)
            ->andReturn($mockLock);

        $mockLock->shouldReceive('block')
            ->once()
            ->andThrow(new LockTimeoutException());

        // Mock cache operations for fallback generation
        Cache::shouldReceive('put')
            ->once()
            ->withArgs(function ($key, $data, $duration) use ($cacheKey) {
                return $key === $cacheKey
                    && is_array($data)
                    && isset($data['token']);
            });

        // Get token - should fall back to generation
        $token = $provider->getToken();

        $this->assertNotEmpty($token);
    }

    public function testDoubleCheckedLockingPreventsRaceCondition()
    {
        // This test verifies that double-checked locking works correctly
        $provider = new ThreadSafeTokenProvider($this->config);

        $cacheKey = 'snowflake_api_token:test_account:test_user';
        $lockKey = 'snowflake_api_token_lock:test_account:test_user';
        $existingToken = 'existing_token.from.another_process';

        // First cache check - miss
        Cache::shouldReceive('get')
            ->once()
            ->with($cacheKey)
            ->andReturn(null);

        // Mock successful lock acquisition
        $mockLock = Mockery::mock(\Illuminate\Contracts\Cache\Lock::class);
        Cache::shouldReceive('lock')
            ->once()
            ->with($lockKey, 5)
            ->andReturn($mockLock);

        // Simulate another process generated token while we waited for lock
        $mockLock->shouldReceive('block')
            ->once()
            ->with(5, Mockery::type('callable'))
            ->andReturnUsing(function ($timeout, $callback) use ($cacheKey, $existingToken) {
                // Simulate token appearing in cache during lock wait
                Cache::shouldReceive('get')
                    ->once()
                    ->with($cacheKey)
                    ->andReturn([
                        'token' => $existingToken,
                        'expiry' => time() + 3600,
                    ]);

                return $callback();
            });

        // Get token - should return existing token, not generate new one
        $token = $provider->getToken();

        $this->assertEquals($existingToken, $token);
    }

    public function testExpiryBufferPreventsExpiredTokens()
    {
        // This test verifies expiry buffer prevents using nearly-expired tokens
        $expiryBuffer = 120; // 2 minutes
        $provider = new ThreadSafeTokenProvider($this->config, $expiryBuffer);

        $cacheKey = 'snowflake_api_token:test_account:test_user';

        // Create token that expires in 1 minute (within buffer)
        $almostExpiredToken = 'almost_expired_token.test.signature';
        $cachedData = [
            'token' => $almostExpiredToken,
            'expiry' => time() + 60, // Expires in 1 minute
        ];

        // Mock cache returning almost-expired token
        Cache::shouldReceive('get')
            ->with($cacheKey)
            ->andReturn($cachedData);

        // Mock token generation since cached token should be rejected
        Cache::shouldReceive('forget')
            ->once()
            ->with($cacheKey);

        $mockLock = Mockery::mock(\Illuminate\Contracts\Cache\Lock::class);
        Cache::shouldReceive('lock')
            ->andReturn($mockLock);

        $mockLock->shouldReceive('block')
            ->andThrow(new Exception('Lock not supported'));

        Cache::shouldReceive('get')
            ->with($cacheKey)
            ->andReturn(null);

        Cache::shouldReceive('put')
            ->once();

        // Get token - should generate new one, not use almost-expired
        $token = $provider->getToken();

        $this->assertNotEmpty($token);
        $this->assertNotEquals($almostExpiredToken, $token);
    }

    public function testClearTokenCacheRemovesAllCaches()
    {
        // This test verifies clearTokenCache removes from all cache levels
        $provider = new ThreadSafeTokenProvider($this->config);

        $cacheKey = 'snowflake_api_token:test_account:test_user';

        // Pre-populate static cache
        $reflection = new \ReflectionClass($provider);
        $staticCache = $reflection->getProperty('staticCache');
        $staticCache->setAccessible(true);
        $staticCache->setValue([
            'test_account:test_user' => [
                'token' => 'test_token',
                'expiry' => time() + 3600,
            ],
        ]);

        // Mock Laravel cache forget
        Cache::shouldReceive('forget')
            ->once()
            ->with($cacheKey);

        // Clear cache
        $provider->clearTokenCache();

        // Verify static cache is empty
        $staticCacheValue = $staticCache->getValue();
        $this->assertArrayNotHasKey('test_account:test_user', $staticCacheValue);
    }

    public function testValidateExpiryBufferEnforcesMinimum()
    {
        // This test verifies expiry buffer validation
        $provider = new ThreadSafeTokenProvider($this->config);

        $this->expectException(SnowflakeApiException::class);
        $this->expectExceptionMessage('Token expiry buffer must be at least 30 seconds');

        $reflection = new \ReflectionClass($provider);
        $method = $reflection->getMethod('validateExpiryBuffer');
        $method->setAccessible(true);
        $method->invoke($provider, 10); // Too low
    }

    public function testValidateExpiryBufferEnforcesMaximum()
    {
        // This test verifies expiry buffer validation
        $provider = new ThreadSafeTokenProvider($this->config);

        $this->expectException(SnowflakeApiException::class);
        $this->expectExceptionMessage('Token expiry buffer cannot exceed 600 seconds');

        $reflection = new \ReflectionClass($provider);
        $method = $reflection->getMethod('validateExpiryBuffer');
        $method->setAccessible(true);
        $method->invoke($provider, 700); // Too high
    }

    /**
     * Get a test RSA private key for JWT generation testing
     *
     * @return string PEM-encoded private key
     */
    private function getTestPrivateKey(): string
    {
        // This is a test-only RSA private key (2048-bit)
        // DO NOT use in production
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
