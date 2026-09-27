<?php

declare(strict_types=1);

namespace Tests\Unit\Services;

use Illuminate\Support\Facades\Cache;
use LaravelSnowflakeApi\Exceptions\StatementOutcomeUnknownException;
use LaravelSnowflakeApi\Services\DmlResult;
use LaravelSnowflakeApi\Services\SnowflakeService;
use LaravelSnowflakeApi\Services\ThreadSafeTokenProvider;
use Mockery;
use Symfony\Component\HttpClient\MockHttpClient;
use Symfony\Component\HttpClient\Response\MockResponse;
use Tests\TestCase;

/**
 * Covers SnowflakeService::executeDml(): the trustworthy-row-count /
 * unknown-outcome contract described in the "DML row counts and unknown
 * outcomes" README section.
 */
class SnowflakeServiceDmlTest extends TestCase
{
    private const STATEMENT_ID = '01c3e62c-0a10-e0a3-0000-066153113e12';

    private const DML_STATEMENT = 'UPDATE t SET x = x + 1, WRITE_VERSION = WRITE_VERSION + 1 WHERE id = 42 AND WRITE_VERSION = 7';

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

    public function test_stats_are_preferred_over_an_output_row_when_both_are_present(): void
    {
        $mockClient = $this->dispatcher([
            $this->acceptedSubmitResponse(),
            $this->executedResponse([
                'rowType' => [['name' => 'number of rows updated', 'type' => 'FIXED']],
                // The output row also claims 5 — stats (2) must win.
                'data' => [['5']],
                'stats' => ['numRowsInserted' => 0, 'numRowsUpdated' => 2, 'numRowsDeleted' => 0],
            ]),
        ]);

        $result = $this->createServiceWithMockedDeps($mockClient)->executeDml(self::DML_STATEMENT);

        $this->assertInstanceOf(DmlResult::class, $result);
        $this->assertSame(0, $result->getInserted());
        $this->assertSame(2, $result->getUpdated());
        $this->assertSame(0, $result->getDeleted());
        $this->assertSame(2, $result->affectedRows());
    }

    public function test_counts_come_from_the_output_row_when_no_stats_are_present(): void
    {
        $mockClient = $this->dispatcher([
            $this->acceptedSubmitResponse(),
            $this->executedResponse([
                'rowType' => [['name' => 'number of rows updated', 'type' => 'FIXED']],
                'data' => [['1']],
                // No 'stats' key at all.
            ]),
        ]);

        $result = $this->createServiceWithMockedDeps($mockClient)->executeDml(self::DML_STATEMENT);

        $this->assertSame(1, $result->getUpdated());
        $this->assertSame(1, $result->affectedRows());
    }

    public function test_throws_counts_unreadable_when_neither_stats_nor_a_recognized_row_are_present(): void
    {
        $mockClient = $this->dispatcher([
            $this->acceptedSubmitResponse(),
            $this->executedResponse([
                'rowType' => [['name' => 'col1', 'type' => 'TEXT']],
                'data' => [['unrelated value']],
            ]),
        ]);

        try {
            $this->createServiceWithMockedDeps($mockClient)->executeDml(self::DML_STATEMENT);
            $this->fail('Expected a StatementOutcomeUnknownException.');
        } catch (StatementOutcomeUnknownException $e) {
            $this->assertSame(self::STATEMENT_ID, $e->getStatementHandle());
        }
    }

    public function test_a_4xx_submission_refusal_propagates_unchanged(): void
    {
        $mockClient = $this->dispatcher([
            new MockResponse(
                json_encode(['code' => '390100', 'message' => 'SQL compilation error: invalid identifier']),
                ['http_code' => 422]
            ),
        ]);

        $this->expectException(\Exception::class);
        $this->expectExceptionCode(422);
        $this->expectExceptionMessageMatches('/SQL compilation error/');

        $this->createServiceWithMockedDeps($mockClient)->executeDml(self::DML_STATEMENT);
    }

    public function test_a_transport_failure_while_submitting_is_reported_as_unknown(): void
    {
        $mockClient = $this->dispatcher([
            new MockResponse('', ['error' => 'simulated transport failure']),
        ]);

        try {
            $this->createServiceWithMockedDeps($mockClient)->executeDml(self::DML_STATEMENT);
            $this->fail('Expected a StatementOutcomeUnknownException.');
        } catch (StatementOutcomeUnknownException $e) {
            $this->assertNull($e->getStatementHandle(), 'no handle was ever assigned — submission itself failed');
            $this->assertInstanceOf(\Throwable::class, $e->getPrevious());
        }
    }

    public function test_a_4xx_polling_refusal_propagates_unchanged(): void
    {
        $mockClient = $this->dispatcher([
            $this->acceptedSubmitResponse(),
            new MockResponse(
                json_encode(['code' => '002003', 'message' => 'SQL execution error: statement failed']),
                ['http_code' => 422]
            ),
        ]);

        $this->expectException(\Exception::class);
        $this->expectExceptionCode(422);
        $this->expectExceptionMessageMatches('/SQL execution error/');

        $this->createServiceWithMockedDeps($mockClient)->executeDml(self::DML_STATEMENT);
    }

    public function test_a_transport_failure_while_polling_is_reported_as_unknown(): void
    {
        $mockClient = $this->dispatcher([
            $this->acceptedSubmitResponse(),
            new MockResponse('', ['error' => 'simulated transport failure']),
        ]);

        try {
            $this->createServiceWithMockedDeps($mockClient)->executeDml(self::DML_STATEMENT);
            $this->fail('Expected a StatementOutcomeUnknownException.');
        } catch (StatementOutcomeUnknownException $e) {
            $this->assertSame(self::STATEMENT_ID, $e->getStatementHandle());
        }
    }

    public function test_the_timeout_cancel_path_is_reported_as_unknown_and_requests_a_cancel(): void
    {
        $cancelRequested = false;

        $mockClient = new MockHttpClient(function (string $method, string $url, array $options) use (&$cancelRequested) {
            if (str_contains($url, '/cancel')) {
                $cancelRequested = true;

                return new MockResponse('{}', ['http_code' => 200]);
            }

            if ($method === 'GET') {
                // Still executing — not the CODE_SUCCESS ('090001') the
                // service checks for, so isExecuted() stays false and the
                // while loop in waitForStatement() is entered.
                return new MockResponse(json_encode([
                    'code' => '333334',
                    'statementHandle' => self::STATEMENT_ID,
                ]), ['http_code' => 200]);
            }

            return $this->acceptedSubmitResponse();
        });

        // Timeout of 0 makes waitForStatement() cancel on its very first
        // loop iteration (elapsed time is always >= 0), with no real sleep.
        $service = $this->createServiceWithMockedDeps($mockClient, 0);

        try {
            $service->executeDml(self::DML_STATEMENT);
            $this->fail('Expected a StatementOutcomeUnknownException.');
        } catch (StatementOutcomeUnknownException $e) {
            $this->assertSame(self::STATEMENT_ID, $e->getStatementHandle());
        }

        $this->assertTrue($cancelRequested, 'waitForStatement() must have requested a cancel on timeout');
    }

    /**
     * @param  array<int, MockResponse>  $responses
     */
    private function dispatcher(array $responses): MockHttpClient
    {
        return new MockHttpClient($responses);
    }

    private function acceptedSubmitResponse(): MockResponse
    {
        return new MockResponse(json_encode([
            'statementHandle' => self::STATEMENT_ID,
            'code' => '333334',
            'message' => 'Statement executed successfully.',
            'statementStatusUrl' => '/api/v2/statements/'.self::STATEMENT_ID,
        ]), ['http_code' => 200]);
    }

    /**
     * A CODE_SUCCESS ('090001') response with sane resultSetMetaData
     * defaults, overridable per test.
     *
     * @param  array<string, mixed>  $overrides
     */
    private function executedResponse(array $overrides): MockResponse
    {
        $rowType = $overrides['rowType'] ?? [];
        $data = $overrides['data'] ?? [];

        $body = [
            'code' => '090001',
            'statementHandle' => self::STATEMENT_ID,
            'data' => $data,
            'resultSetMetaData' => [
                'numRows' => count($data),
                'partitionInfo' => [0],
                'rowType' => $rowType,
            ],
            'createdOn' => time(),
        ];

        if (array_key_exists('stats', $overrides)) {
            $body['stats'] = $overrides['stats'];
        }

        return new MockResponse(json_encode($body), ['http_code' => 200]);
    }

    private function createServiceWithMockedDeps(MockHttpClient $mockClient, int $timeout = 30): SnowflakeService
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
            $timeout
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
