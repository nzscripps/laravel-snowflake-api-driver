<?php

declare(strict_types=1);

namespace Tests\Unit;

use Illuminate\Database\Connection as BaseConnection;
use Illuminate\Database\QueryException;
use Illuminate\Support\Facades\Cache;
use LaravelSnowflakeApi\Exceptions\StatementOutcomeUnknownException;
use LaravelSnowflakeApi\Services\SnowflakeService;
use LaravelSnowflakeApi\Services\ThreadSafeTokenProvider;
use LaravelSnowflakeApi\SnowflakeApiConnection;
use Mockery;
use RuntimeException;
use Symfony\Component\HttpClient\MockHttpClient;
use Symfony\Component\HttpClient\Response\MockResponse;
use Tests\TestCase;

/**
 * Covers SnowflakeApiConnection::affectingStatement(): the Laravel-facing
 * side of the DML row-count / unknown-outcome contract (statement()/
 * update()/delete() are unchanged and are not re-tested here).
 */
class SnowflakeApiConnectionAffectingStatementTest extends TestCase
{
    private const STATEMENT_ID = '01c3e62c-0a10-e0a3-0000-066153113e12';

    private const DML_STATEMENT = 'UPDATE t SET x = x + 1, WRITE_VERSION = WRITE_VERSION + 1 WHERE id = ? AND WRITE_VERSION = ?';

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

    public function test_affecting_statement_returns_the_affected_row_count(): void
    {
        $mockClient = new MockHttpClient([
            $this->acceptedSubmitResponse(),
            $this->executedResponse(),
        ]);

        $connection = $this->connectionWithMockedService($mockClient);

        $count = $connection->affectingStatement(self::DML_STATEMENT, [42, 7]);

        $this->assertSame(3, $count);
    }

    public function test_pretending_returns_zero_without_calling_snowflake(): void
    {
        $mockClient = new MockHttpClient(function (): MockResponse {
            throw new RuntimeException('SnowflakeService must not be called while pretending.');
        });

        $connection = $this->connectionWithMockedService($mockClient);

        $count = null;
        $connection->pretend(function ($conn) use (&$count) {
            $count = $conn->affectingStatement(self::DML_STATEMENT, [42, 7]);
        });

        $this->assertSame(0, $count);
    }

    public function test_an_unknown_outcome_surfaces_as_query_exception_and_submits_exactly_once(): void
    {
        $submitCount = 0;

        $mockClient = new MockHttpClient(function () use (&$submitCount) {
            $submitCount++;

            return new MockResponse('', ['error' => 'simulated transport failure']);
        });

        $connection = $this->connectionWithMockedService($mockClient);

        try {
            $connection->affectingStatement(self::DML_STATEMENT, [42, 7]);
            $this->fail('Expected a QueryException.');
        } catch (QueryException $e) {
            $this->assertInstanceOf(StatementOutcomeUnknownException::class, $e->getPrevious());
        }

        // Laravel's Connection::run() only retries a query when the
        // *original* exception's message matches a lost-connection pattern
        // (Illuminate\Database\LostConnectionDetector). StatementOutcomeUnknownException's
        // fixed message never does (see its class docblock), so the
        // statement must not have been silently re-submitted.
        $this->assertSame(1, $submitCount, 'Laravel must not silently re-submit a DML statement with an unknown outcome');
    }

    private function connectionWithMockedService(MockHttpClient $mockClient): SnowflakeApiConnection
    {
        $connection = new class extends SnowflakeApiConnection
        {
            public function __construct()
            {
                // Skip SnowflakeApiConnection::__construct() (it needs live
                // Snowflake config and calls testConnection()) and go
                // straight to the base Connection constructor. That only
                // needs a non-null $pdo — reconnectIfMissingConnection()
                // checks is_null($this->pdo), never getPdo() — and it sets
                // up the query grammar/post processor via the overrides
                // SnowflakeApiConnectionTest.php already exercises, none of
                // which touch the network.
                BaseConnection::__construct(new \stdClass, 'test_database', '', []);
            }
        };

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

        $this->setPrivateProperty($connection, 'snowflakeService', $service);

        return $connection;
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

    private function executedResponse(): MockResponse
    {
        return new MockResponse(json_encode([
            'code' => '090001',
            'statementHandle' => self::STATEMENT_ID,
            'data' => [['3']],
            'resultSetMetaData' => [
                'numRows' => 1,
                'partitionInfo' => [0],
                'rowType' => [['name' => 'number of rows updated', 'type' => 'FIXED']],
            ],
            'createdOn' => time(),
        ]), ['http_code' => 200]);
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
