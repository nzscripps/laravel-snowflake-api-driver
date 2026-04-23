<?php

declare(strict_types=1);

namespace Tests\Unit\Services;

use Illuminate\Support\Facades\Cache;
use LaravelSnowflakeApi\Services\SnowflakeService;
use LaravelSnowflakeApi\Services\ThreadSafeTokenProvider;
use Mockery;
use ReflectionMethod;
use Symfony\Component\HttpClient\MockHttpClient;
use Symfony\Component\HttpClient\Response\MockResponse;
use Tests\TestCase;

/**
 * Verify that cancelStatement() sends the request shape Snowflake's
 * /api/v2/statements/{id}/cancel endpoint requires.
 *
 * Snowflake returns HTTP 415 Unsupported Media Type if the POST lacks
 * Content-Type: application/json with a JSON body. This test locks in
 * that the driver always sends both.
 */
class SnowflakeServiceCancelTest extends TestCase
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

    public function test_cancel_statement_sends_json_content_type_and_empty_body(): void
    {
        $capturedMethod = null;
        $capturedUrl = null;
        $capturedOptions = null;

        $mockClient = new MockHttpClient(function (string $method, string $url, array $options) use (
            &$capturedMethod,
            &$capturedUrl,
            &$capturedOptions,
        ) {
            $capturedMethod = $method;
            $capturedUrl = $url;
            $capturedOptions = $options;

            return new MockResponse('', ['http_code' => 200]);
        });

        $service = $this->createServiceWithMockedDeps($mockClient);

        $service->cancelStatement('01c3e62c-0a10-e0a3-0000-066153113e12');

        $this->assertSame('POST', $capturedMethod);
        $this->assertStringContainsString(
            '/api/v2/statements/01c3e62c-0a10-e0a3-0000-066153113e12/cancel',
            $capturedUrl
        );

        // Symfony normalizes 'Content-Type: application/json' into
        // $options['headers']['content-type'] = ['application/json'].
        $this->assertNotEmpty($capturedOptions['headers'] ?? null, 'request must set headers');
        $headerBlob = is_array($capturedOptions['headers'])
            ? strtolower(implode("\n", array_map(
                fn ($v) => is_array($v) ? implode(',', $v) : (string) $v,
                $capturedOptions['headers'],
            )))
            : strtolower((string) $capturedOptions['headers']);
        $this->assertStringContainsString(
            'content-type: application/json',
            $headerBlob,
            'Snowflake cancel endpoint requires Content-Type: application/json to avoid HTTP 415'
        );

        $this->assertSame(
            '{}',
            $capturedOptions['body'] ?? null,
            'Snowflake cancel endpoint requires a JSON body; empty {} is accepted'
        );
    }

    public function test_cancel_statement_throws_on_non_2xx_status(): void
    {
        $mockClient = new MockHttpClient(
            fn () => new MockResponse('', ['http_code' => 415])
        );

        $service = $this->createServiceWithMockedDeps($mockClient);

        $this->expectException(\Exception::class);
        $this->expectExceptionMessage('Failed to cancel statement, received status code: 415');

        $service->cancelStatement('stmt-id');
    }

    private function createServiceWithMockedDeps(MockHttpClient $mockClient): SnowflakeService
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

    private function getTestPrivateKey(): string
    {
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
}
