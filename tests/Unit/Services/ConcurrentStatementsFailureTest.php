<?php

declare(strict_types=1);

namespace Tests\Unit\Services;

use LaravelSnowflakeApi\Services\ConcurrentStatements;
use PHPUnit\Framework\TestCase;
use RuntimeException;

/**
 * The failure rule of ConcurrentStatements::run() holds on every PHP version:
 * with Fibers and in the one-after-another fallback of PHP 8.0.
 */
class ConcurrentStatementsFailureTest extends TestCase
{
    public function test_every_task_runs_before_the_first_failure_is_rethrown(): void
    {
        $ran = [];

        try {
            ConcurrentStatements::run([
                'first' => function () use (&$ran) {
                    $ran[] = 'first';

                    throw new RuntimeException('first failed');
                },
                'second' => function () use (&$ran) {
                    $ran[] = 'second';

                    throw new RuntimeException('second failed');
                },
                'third' => function () use (&$ran) {
                    $ran[] = 'third';

                    return 3;
                },
            ]);
            $this->fail('the failure of a task must reach the caller');
        } catch (RuntimeException $e) {
            $this->assertSame('first failed', $e->getMessage(), 'the first failure in task order wins');
        }

        $this->assertSame(['first', 'second', 'third'], $ran);
    }

    public function test_a_single_failing_task_fails_the_same_way(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('only task failed');

        ConcurrentStatements::run(['only' => function () {
            throw new RuntimeException('only task failed');
        }]);
    }
}
