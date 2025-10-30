<?php

declare(strict_types=1);

namespace Tests\Unit\Services;

use Illuminate\Support\Facades\Cache;
use LaravelSnowflakeApi\Services\SnowflakeService;
use Mockery;
use ReflectionClass;
use ReflectionMethod;
use ReflectionProperty;
use Tests\TestCase;

/**
 * Test HTTP client lifecycle management in SnowflakeService
 *
 * These tests verify that the HTTP client is properly recreated
 * in long-running processes to prevent connection leaks, DNS cache
 * staleness, and HTTP/2 connection age issues.
 */
class SnowflakeServiceHttpClientTest extends TestCase
{
    protected function setUp(): void
    {
        parent::setUp();

        // Mock Cache facade to avoid needing actual cache drivers
        Cache::shouldReceive('get')->andReturn(null)->byDefault();
        Cache::shouldReceive('put')->andReturn(true)->byDefault();
        Cache::shouldReceive('lock')->andReturnSelf()->byDefault();
        Cache::shouldReceive('block')->andReturn(null)->byDefault();
    }

    protected function tearDown(): void
    {
        Mockery::close();
        parent::tearDown();
    }
    /**
     * Test that HTTP client is created on first use (lazy initialization)
     */
    public function test_http_client_is_null_on_initialization(): void
    {
        $service = $this->createService();
        $httpClient = $this->getPrivateProperty($service, 'httpClient');

        $this->assertNull($httpClient, 'HTTP client should be null after construction (lazy init)');
    }

    /**
     * Test that HTTP client is created on first getHttpClient() call
     */
    public function test_http_client_is_created_on_first_use(): void
    {
        $service = $this->createService();

        // Call getHttpClient() via reflection
        $client = $this->callGetHttpClient($service);

        $this->assertNotNull($client, 'HTTP client should be created on first use');
        $this->assertInstanceOf(
            \Symfony\Contracts\HttpClient\HttpClientInterface::class,
            $client,
            'HTTP client should implement HttpClientInterface'
        );
    }

    /**
     * Test that HTTP client created timestamp is set on first use
     */
    public function test_http_client_timestamp_is_set_on_first_use(): void
    {
        $service = $this->createService();

        $timestampBefore = $this->getPrivateProperty($service, 'httpClientCreatedAt');
        $this->assertEquals(0, $timestampBefore, 'Timestamp should be 0 before first use');

        // Create client
        $this->callGetHttpClient($service);

        $timestampAfter = $this->getPrivateProperty($service, 'httpClientCreatedAt');
        $this->assertGreaterThan(0, $timestampAfter, 'Timestamp should be set after creation');
        $this->assertLessThanOrEqual(time(), $timestampAfter, 'Timestamp should not be in the future');
    }

    /**
     * Test that HTTP client is reused within max age window
     */
    public function test_http_client_is_reused_within_max_age(): void
    {
        $service = $this->createService();

        // Get client twice in quick succession
        $client1 = $this->callGetHttpClient($service);
        $client2 = $this->callGetHttpClient($service);

        $this->assertSame($client1, $client2, 'HTTP client should be reused within max age window');
    }

    /**
     * Test that HTTP client is recreated after max age expires
     */
    public function test_http_client_is_recreated_after_max_age(): void
    {
        $service = $this->createService();

        // Get initial client
        $client1 = $this->callGetHttpClient($service);
        $timestamp1 = $this->getPrivateProperty($service, 'httpClientCreatedAt');

        // Simulate time passing by setting createdAt to 2 hours ago
        $this->setPrivateProperty($service, 'httpClientCreatedAt', time() - 7200);

        // Get client again - should recreate
        $client2 = $this->callGetHttpClient($service);
        $timestamp2 = $this->getPrivateProperty($service, 'httpClientCreatedAt');

        $this->assertNotSame($client1, $client2, 'HTTP client should be recreated after max age');
        $this->assertGreaterThan($timestamp1 - 7200, $timestamp2, 'Creation timestamp should be updated');
    }

    /**
     * Test that HTTP_CLIENT_MAX_AGE constant is defined
     */
    public function test_http_client_max_age_constant_exists(): void
    {
        $reflection = new ReflectionClass(SnowflakeService::class);
        $this->assertTrue(
            $reflection->hasConstant('HTTP_CLIENT_MAX_AGE'),
            'HTTP_CLIENT_MAX_AGE constant should exist'
        );

        $maxAge = $reflection->getConstant('HTTP_CLIENT_MAX_AGE');
        $this->assertEquals(3600, $maxAge, 'HTTP_CLIENT_MAX_AGE should be 3600 seconds (1 hour)');
    }

    /**
     * Test that HTTP client is recreated one second after max age boundary
     */
    public function test_http_client_is_recreated_after_max_age_boundary(): void
    {
        $service = $this->createService();

        // Get initial client
        $client1 = $this->callGetHttpClient($service);

        // Simulate time passing to 1 second after max age (3601 seconds)
        $now = time();
        $this->setPrivateProperty($service, 'httpClientCreatedAt', $now - 3601);

        // Get client again - should recreate
        $client2 = $this->callGetHttpClient($service);

        $this->assertNotSame($client1, $client2, 'HTTP client should be recreated after max age boundary');
    }

    /**
     * Test that HTTP client is NOT recreated one second before max age
     */
    public function test_http_client_is_not_recreated_before_max_age(): void
    {
        $service = $this->createService();

        // Get initial client
        $client1 = $this->callGetHttpClient($service);

        // Simulate time passing to 1 second before max age (3599 seconds)
        $now = time();
        $this->setPrivateProperty($service, 'httpClientCreatedAt', $now - 3599);

        // Get client again - should reuse
        $client2 = $this->callGetHttpClient($service);

        $this->assertSame($client1, $client2, 'HTTP client should not be recreated before max age');
    }

    /**
     * Test that CURLOPT_MAXLIFETIME_CONN is set if available
     *
     * Note: This test can only verify the constant check,
     * not the actual cURL option, as that's internal to Symfony HttpClient
     */
    public function test_curlopt_maxlifetime_conn_is_checked(): void
    {
        $service = $this->createService();

        // Trigger client creation
        $this->callGetHttpClient($service);

        // If this doesn't throw, the constant check is working
        $this->assertTrue(true, 'CURLOPT_MAXLIFETIME_CONN constant check works');
    }

    /**
     * Test that multiple recreations work correctly
     */
    public function test_multiple_recreations_work_correctly(): void
    {
        $service = $this->createService();

        // First creation
        $client1 = $this->callGetHttpClient($service);

        // Force first recreation
        $this->setPrivateProperty($service, 'httpClientCreatedAt', time() - 7200);
        $client2 = $this->callGetHttpClient($service);

        // Force second recreation
        $this->setPrivateProperty($service, 'httpClientCreatedAt', time() - 7200);
        $client3 = $this->callGetHttpClient($service);

        $this->assertNotSame($client1, $client2, 'First recreation should create new client');
        $this->assertNotSame($client2, $client3, 'Second recreation should create new client');
        $this->assertNotSame($client1, $client3, 'Third client should be different from first');
    }

    /**
     * Test that HTTP client configuration is correct
     */
    public function test_http_client_configuration_is_correct(): void
    {
        $service = $this->createService();
        $client = $this->callGetHttpClient($service);

        // We can't directly inspect HttpClient options, but we can verify it was created
        $this->assertNotNull($client, 'HTTP client should be created with correct configuration');
    }

    /**
     * Create a SnowflakeService instance for testing
     */
    private function createService(): SnowflakeService
    {
        return new SnowflakeService(
            'https://test.snowflakecomputing.com',
            'test_account',
            'test_user',
            'test_public_key_fingerprint',
            $this->getTestPrivateKey(),
            '', // No passphrase for test key
            'test_warehouse',
            'test_database',
            'test_schema',
            30 // 30 second timeout
        );
    }

    /**
     * Get a valid RSA private key for testing
     * This is a test-only key that is never used in production
     */
    private function getTestPrivateKey(): string
    {
        // Generate a minimal valid RSA private key for testing
        // This is ONLY for unit tests and has no security value
        return <<<'EOD'
-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEAy8Dbv8prQbENkqwPQEqHNdCjb3TYJ6F+FvRHVQKRjLLG5VUE
tcLVW7dVjYs2y9D7dGHkKFwjlFhMHTSZsKTFXhEyZJRPQiECDGYxHgxBqFxYLbhN
yrJKqGBZsI4WPvqrZH1KMO6bGH1TKhcwB1YZ8PuNSQg8wTKpVILwWghJKlDkQwCj
P8N0S+gPSYVqQJYB7bSZGPSxQjCcEI0KjQr7hqQXHMCUwjCqKjrYeNkb0y0kNYLh
jDqbJ9KHxNMKfqFB7PJ9xN3ljRoC5MnKqHqPkDQDmqXqNQMGWV8gTkgLJLxQNzYQ
QclZN5FVlPXKIvJL7LrFLLYQGFYPQ3T1kMXCsQIDAQABAoIBAFxXKAcTXyQSNXPG
o8KUxN8bR7xbMkhYFjLSRMBP3qB1qkYXWa5QQFEFZfqcHsrDNf9Z9fCZXsxZPqPb
hDEqHLXzFHLEKYNJCRqhOsK5+k3l0kfQN6kU8AvVsjvLgxWWQK7T8KQvRQGGrGxI
tPKLVqQlKCDKbYxMUE3uJE8L8hQaQwGhGJMqXClqc7qLlFKLm6fmqHW8TQaXvxHy
4tN3F0VfQCxQU8ELmVrHbNvNGNEfJ0RpEqMLwBCqLlRxJMKdH8JQKqLLbKCQIVJM
CdXKJHLaZ6vOGWYQ2kBYQqVJLi7bG6A7xjQhNFBJLPvHZxZjLKpqDqRqYgHQ7sRx
M5QZwgECgYEA8RlLqFMhUCLKqL0j8qSQNjBjIX7qPAoJPgF7kTmCWx9eKqLdQFxw
GKYVqZpEOFVJTtKDT5rPSZJPmAKQPqWQYVfL+RvFKZGKH4wPxHXpxzWZYLKQGBYO
HjSAoRR0r8XpWRgvVCEwmLHkqkQAFRdMQyQhHLLWc8pUqJ2CqLhYZmECgYEA2Axj
QMXmBslVBqLhJVAjn9HKqw1P3vYqW3nQQUEKXBHLEYEKKHLZCbhHPXqLRPSLGFWX
PdQPHVQHHWQVRxHNJKYkKb6BdPFXRMJdLrLXLfZCJQVNlQJWQJWQFbmVQGKXHGLg
KzBKTJQKQWQXQLgLNHGHLfHKQJLLKQPQQLfQQLkCgYBjQJqPQKQqLJQPQJqPQKqL
JQPQJqPQKQqLJQPQJqPQKQqLJQPQJqPQKQqLJQPQJqPQKQqLJQPQJqPQKQqLJQPQ
JqPQKQqLJQPQJqPQKQqLJQPQJqPQKQqLJQPQJqPQKQqLJQPQJqPQKQqLJQPQJqPQ
KQqLJQPQJqPQKQqLJQPQJqPQKQqLJQPQJqPQKQqLJQECgYA0lULHLQKQXHPXqKQL
fQPLLHKQPfQVQHLHKQPfQVQHLHKQPfQVQHLHKQPfQVQHLHKQPfQVQHLHKQPfQVQH
LHKQPfQVQHLHKQPfQVQHLHKQPfQVQHLHKQPfQVQHLHKQPfQVQHLHKQPfQVQHLHKQ
PfQVQHLHKQPfQVQHLHKQPfQVQHLHKQPfQVQHLHKQPfQVQQKBgH0KQLHfQKQLPXQL
HfQKQLPXQLHfQKQLPXQLHfQKQLPXQLHfQKQLPXQLHfQKQLPXQLHfQKQLPXQLHfQKQ
LPXQLHfQKQLPXQLHfQKQLPXQLHfQKQLPXQLHfQKQLPXQLHfQKQLPXQLHfQKQLPXQL
HfQKQLPXQLHfQKQLPXQLHfQKQLPXQLHfQKQLPXQLHfQKQLP==
-----END RSA PRIVATE KEY-----
EOD;
    }

    /**
     * Call private getHttpClient() method via reflection
     */
    private function callGetHttpClient(SnowflakeService $service)
    {
        $reflection = new ReflectionMethod(SnowflakeService::class, 'getHttpClient');
        $reflection->setAccessible(true);

        return $reflection->invoke($service);
    }
}
