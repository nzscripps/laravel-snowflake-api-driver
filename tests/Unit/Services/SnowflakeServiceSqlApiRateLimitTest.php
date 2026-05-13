<?php

declare(strict_types=1);

namespace Tests\Unit\Services;

use Exception;
use Illuminate\Support\Facades\Cache;
use LaravelSnowflakeApi\Services\SnowflakeService;
use LaravelSnowflakeApi\Services\ThreadSafeTokenProvider;
use Mockery;
use ReflectionMethod;
use Symfony\Component\HttpClient\MockHttpClient;
use Symfony\Component\HttpClient\Response\MockResponse;
use Tests\TestCase;

/**
 * SQL API cloud-service throttling returns HTTP 429 and/or "Request rate exceeds max".
 * The driver must retry with exponential backoff (Snowflake documentation).
 */
class SnowflakeServiceSqlApiRateLimitTest extends TestCase
{
    protected function setUp(): void
    {
        parent::setUp();

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

    public function test_make_request_retries_after_http_429_then_succeeds(): void
    {
        $body429 = json_encode([
            'message' => 'Request rate exceeds max. Try later.',
            'code' => '390908',
        ], JSON_THROW_ON_ERROR);

        $body200 = json_encode(['ok' => true, 'recovered' => 1], JSON_THROW_ON_ERROR);

        $mockClient = new MockHttpClient([
            new MockResponse($body429, ['http_code' => 429]),
            new MockResponse($body200, ['http_code' => 200]),
        ]);

        $service = $this->createServiceWithMockHttpClient($mockClient);

        $started = microtime(true);
        $result = $this->invokeMakeRequest($service, 'GET', 'https://test_account.snowflakecomputing.com/api/v2/probe');
        $elapsedMs = (microtime(true) - $started) * 1000;

        $this->assertSame(1, $result['recovered'] ?? null);
        $this->assertGreaterThanOrEqual(1500, $elapsedMs, 'first backoff should be ~2000ms before second attempt');
    }

    public function test_make_request_does_not_retry_on_http_400(): void
    {
        $calls = 0;
        $mockClient = new MockHttpClient(function () use (&$calls) {
            $calls++;
            $body = json_encode(['message' => 'Bad request', 'code' => 'bad'], JSON_THROW_ON_ERROR);

            return new MockResponse($body, ['http_code' => 400]);
        });

        $service = $this->createServiceWithMockHttpClient($mockClient);

        try {
            $this->invokeMakeRequest($service, 'GET', 'https://test_account.snowflakecomputing.com/api/v2/probe');
            $this->fail('Expected HTTP 400 to throw');
        } catch (Exception) {
            // expected
        }

        $this->assertSame(1, $calls);
    }

    private function createServiceWithMockHttpClient(MockHttpClient $mockClient): SnowflakeService
    {
        $service = new SnowflakeService(
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

        $tokenProvider = Mockery::mock(ThreadSafeTokenProvider::class);
        $tokenProvider->shouldReceive('getToken')->andReturn('test-jwt-token');
        $this->setPrivateProperty($service, 'tokenProvider', $tokenProvider);

        $this->setPrivateProperty($service, 'httpClient', $mockClient);
        $this->setPrivateProperty($service, 'httpClientCreatedAt', time());

        return $service;
    }

    /**
     * @return array<string, mixed>
     */
    private function invokeMakeRequest(SnowflakeService $service, string $method, string $url): array
    {
        $methodRef = new ReflectionMethod(SnowflakeService::class, 'makeRequest');
        $methodRef->setAccessible(true);

        /** @var array<string, mixed> */
        return $methodRef->invoke($service, $method, $url, []);
    }

    private function getTestPrivateKey(): string
    {
        return <<<'EOD'
-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEAy8Dbv8prQbENkqwPQEqHNdCjb3TYJ6F+FvRHVQKRjLLG5VUE
tcLVW7dVjYs2y9D7dGHkKFwjlFhMHTSZsKTFXhEyJRPQiECDGYxHgxBqFxYLbhN
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
}
