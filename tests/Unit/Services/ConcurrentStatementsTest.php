<?php

declare(strict_types=1);

namespace Tests\Unit\Services;

use Fiber;
use Illuminate\Support\Facades\Cache;
use LaravelSnowflakeApi\Services\ConcurrentStatements;
use LaravelSnowflakeApi\Services\SnowflakeService;
use LaravelSnowflakeApi\Services\ThreadSafeTokenProvider;
use Mockery;
use RuntimeException;
use Symfony\Component\HttpClient\MockHttpClient;
use Symfony\Component\HttpClient\Response\MockResponse;
use Tests\TestCase;

/**
 * ConcurrentStatements::run() overlaps the time independent tasks spend waiting on
 * Snowflake. These tests drive the real SnowflakeService against a scripted SQL API:
 * every statement reports "still running" for its first polls, so a task that waits
 * its turn is visible in the order of requests.
 *
 * @requires PHP >= 8.1
 */
class ConcurrentStatementsTest extends TestCase
{
    /** @var list<string> */
    private array $events = [];

    /** @var array<string, int> polls answered "still running" so far, by statement handle */
    private array $polls = [];

    /** @var array<string, string> statement text by handle */
    private array $statements = [];

    private int $pollsBeforeDone = 2;

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

    public function test_every_task_posts_its_statement_before_any_statement_completes(): void
    {
        $service = $this->service();

        $rows = ConcurrentStatements::run([
            'a' => fn () => $service->ExecuteQuery('SELECT A'),
            'b' => fn () => $service->ExecuteQuery('SELECT B'),
            'c' => fn () => $service->ExecuteQuery('SELECT C'),
        ]);

        $this->assertSame(['a', 'b', 'c'], array_keys($rows));
        $this->assertSame('SELECT A', $rows['a']->first()['SQL']);
        $this->assertSame('SELECT B', $rows['b']->first()['SQL']);
        $this->assertSame('SELECT C', $rows['c']->first()['SQL']);

        $this->assertSame(
            ['post SELECT A', 'post SELECT B', 'post SELECT C'],
            array_slice($this->events, 0, 3),
            'all three statements are running on Snowflake before the first poll',
        );
        $firstDone = $this->firstIndex('done ');
        $this->assertGreaterThan($this->firstIndex('post SELECT C'), $firstDone);
    }

    public function test_statements_inside_one_task_still_run_in_order(): void
    {
        $service = $this->service();

        ConcurrentStatements::run([
            'chain' => function () use ($service) {
                $service->ExecuteQuery('SELECT FIRST');

                return $service->ExecuteQuery('SELECT SECOND');
            },
            'other' => fn () => $service->ExecuteQuery('SELECT OTHER'),
        ]);

        $this->assertLessThan(
            $this->firstIndex('post SELECT SECOND'),
            $this->firstIndex('done SELECT FIRST'),
            'a task that reads its first result before issuing its second keeps that order',
        );
        $this->assertLessThan(
            $this->firstIndex('done SELECT FIRST'),
            $this->firstIndex('post SELECT OTHER'),
            'the other task overlaps the chain',
        );
    }

    public function test_a_failing_task_lets_the_others_finish_and_then_rethrows(): void
    {
        $service = $this->service();
        $finished = [];

        try {
            ConcurrentStatements::run([
                'ok-before' => function () use ($service, &$finished) {
                    $service->ExecuteQuery('SELECT BEFORE');
                    $finished[] = 'ok-before';
                },
                'fails' => function () use ($service) {
                    $service->ExecuteQuery('SELECT SLOW');

                    throw new RuntimeException('segment failed');
                },
                'ok-after' => function () use ($service, &$finished) {
                    $service->ExecuteQuery('SELECT AFTER 1');
                    $service->ExecuteQuery('SELECT AFTER 2');
                    $finished[] = 'ok-after';
                },
            ]);
            $this->fail('the failure of a task must reach the caller');
        } catch (RuntimeException $e) {
            $this->assertSame('segment failed', $e->getMessage());
        }

        $this->assertSame(['ok-before', 'ok-after'], $finished, 'no task is abandoned mid-wait');
        $this->assertContains('done SELECT AFTER 2', $this->events);
    }

    public function test_outside_run_a_statement_wait_does_not_suspend_the_callers_fiber(): void
    {
        $service = $this->service();
        $fiber = new Fiber(fn () => $service->ExecuteQuery('SELECT ALONE'));

        $fiber->start();

        $this->assertTrue($fiber->isTerminated(), 'a fiber the runner does not own is never suspended by the driver');
        $this->assertSame('SELECT ALONE', $fiber->getReturn()->first()['SQL']);
    }

    public function test_a_task_may_run_tasks_of_its_own(): void
    {
        $service = $this->service();

        $rows = ConcurrentStatements::run([
            'outer' => fn () => ConcurrentStatements::run([
                'x' => fn () => $service->ExecuteQuery('SELECT X'),
                'y' => fn () => $service->ExecuteQuery('SELECT Y'),
            ]),
            'z' => fn () => $service->ExecuteQuery('SELECT Z'),
        ]);

        $this->assertSame('SELECT X', $rows['outer']['x']->first()['SQL']);
        $this->assertSame('SELECT Y', $rows['outer']['y']->first()['SQL']);
        $this->assertSame('SELECT Z', $rows['z']->first()['SQL']);
        $this->assertLessThan($this->firstIndex('done '), $this->firstIndex('post SELECT Z'));
    }

    public function test_results_keep_the_task_keys_and_order_whatever_finishes_first(): void
    {
        $this->pollsBeforeDone = 0;
        $service = $this->service();

        $rows = ConcurrentStatements::run([
            'slow' => function () use ($service) {
                $service->ExecuteQuery('SELECT 1');
                $service->ExecuteQuery('SELECT 2');

                return 'slow';
            },
            7 => fn () => 'no statement at all',
        ]);

        $this->assertSame(['slow' => 'slow', 7 => 'no statement at all'], $rows);
        $this->assertSame([], ConcurrentStatements::run([]));
    }

    private function firstIndex(string $prefix): int
    {
        foreach ($this->events as $index => $event) {
            if (str_starts_with($event, $prefix)) {
                return $index;
            }
        }

        $this->fail("no event starts with [{$prefix}]: ".implode(' | ', $this->events));
    }

    private function service(): SnowflakeService
    {
        $client = new MockHttpClient(function (string $method, string $url, array $options): MockResponse {
            if ($method === 'POST') {
                $statement = json_decode((string) $options['body'], true)['statement'];
                $handle = 'h'.(count($this->statements) + 1);
                $this->statements[$handle] = $statement;
                $this->polls[$handle] = 0;
                $this->events[] = "post {$statement}";

                return $this->sqlApiResponse([
                    'code' => '333334',
                    'message' => 'in progress',
                    'statementHandle' => $handle,
                    'statementStatusUrl' => "/api/v2/statements/{$handle}",
                ], 202);
            }

            preg_match('#/statements/([^?/]+)#', $url, $match);
            $handle = $match[1];
            $statement = $this->statements[$handle];

            if ($this->polls[$handle] < $this->pollsBeforeDone) {
                $this->polls[$handle]++;
                $this->events[] = "poll {$statement}";

                return $this->sqlApiResponse(['code' => '333334', 'message' => 'in progress', 'statementHandle' => $handle], 202);
            }

            $this->events[] = "done {$statement}";

            return $this->sqlApiResponse([
                'code' => '090001',
                'message' => 'ok',
                'statementHandle' => $handle,
                'data' => [[$statement]],
                'resultSetMetaData' => [
                    'numRows' => 1,
                    'partitionInfo' => [['rowCount' => 1]],
                    'rowType' => [['name' => 'SQL', 'type' => 'TEXT']],
                ],
                'createdOn' => 1_700_000_000_000,
            ], 200);
        });

        $service = new SnowflakeService(
            'https://test.snowflakecomputing.com',
            'test_account',
            'test_user',
            'test_public_key_fingerprint',
            'unused-private-key',
            '',
            'test_warehouse',
            'test_database',
            'test_schema',
            30
        );

        $tokenProvider = Mockery::mock(ThreadSafeTokenProvider::class);
        $tokenProvider->shouldReceive('getToken')->andReturn('test-jwt-token');
        $this->setPrivateProperty($service, 'tokenProvider', $tokenProvider);
        $this->setPrivateProperty($service, 'httpClient', $client);
        $this->setPrivateProperty($service, 'httpClientCreatedAt', time());

        return $service;
    }

    /**
     * @param  array<string, mixed>  $body
     */
    private function sqlApiResponse(array $body, int $status): MockResponse
    {
        return new MockResponse(json_encode($body), [
            'http_code' => $status,
            'response_headers' => ['content-type' => 'application/json'],
        ]);
    }
}
