<?php

declare(strict_types=1);

namespace LaravelSnowflakeApi\Services;

use Fiber;
use Throwable;
use WeakMap;

/**
 * Overlaps the time independent tasks spend waiting on Snowflake.
 *
 * The SQL API runs every statement asynchronously: the driver posts it and polls
 * until it is done. run() gives each task its own Fiber, and a task hands control
 * to the others only where the driver would otherwise sleep between polls. A task
 * that issues several statements still issues them in order; HTTP calls and all
 * other code in a task run without interleaving.
 *
 * Tasks share whatever objects they close over, so state a task holds across a
 * statement (a lock, a property it restores afterwards) must not be touched by
 * the other tasks.
 */
final class ConcurrentStatements
{
    /**
     * When each fiber run() owns may continue (hrtime nanoseconds).
     *
     * @var WeakMap<Fiber, int>|null
     */
    private static ?WeakMap $wakeAt = null;

    /**
     * Run every task and return their results under the tasks' keys, in the tasks' order.
     *
     * Every task runs to its end even when another one throws, so no task is abandoned
     * in the middle of a statement; the first failure (in task order) is then rethrown.
     * On PHP 8.0, which has no Fibers, the tasks run one after another under the same rule.
     *
     * @template TKey of array-key
     * @template TValue
     *
     * @param  array<TKey, callable(): TValue>  $tasks
     * @return array<TKey, TValue>
     */
    public static function run(array $tasks): array
    {
        $results = [];
        $failures = [];

        if (count($tasks) < 2 || ! class_exists(Fiber::class)) {
            foreach ($tasks as $key => $task) {
                try {
                    $results[$key] = $task();
                } catch (Throwable $failure) {
                    $failures[$key] = $failure;
                }
            }
        } else {
            [$results, $failures] = self::runInFibers($tasks);
        }

        $ordered = [];
        foreach ($tasks as $key => $task) {
            if (isset($failures[$key])) {
                throw $failures[$key];
            }

            $ordered[$key] = $results[$key];
        }

        return $ordered;
    }

    /**
     * @param  array<array-key, callable(): mixed>  $tasks
     * @return array{array<array-key, mixed>, array<array-key, Throwable>} results and failures by task key
     */
    private static function runInFibers(array $tasks): array
    {
        self::$wakeAt ??= new WeakMap;

        $pending = [];
        foreach ($tasks as $key => $task) {
            $pending[$key] = new Fiber($task);
            self::$wakeAt[$pending[$key]] = 0;
        }

        $results = [];
        $failures = [];

        while ($pending !== []) {
            $nextWake = null;

            foreach ($pending as $key => $fiber) {
                try {
                    if (! $fiber->isStarted()) {
                        $fiber->start();
                    } elseif (self::$wakeAt[$fiber] <= hrtime(true)) {
                        $fiber->resume();
                    }
                } catch (Throwable $failure) {
                    $failures[$key] = $failure;
                    unset($pending[$key]);

                    continue;
                }

                if ($fiber->isTerminated()) {
                    $results[$key] = $fiber->getReturn();
                    unset($pending[$key]);

                    continue;
                }

                $nextWake = min($nextWake ?? PHP_INT_MAX, self::$wakeAt[$fiber]);
            }

            if ($nextWake !== null) {
                self::pause(intdiv(max(0, $nextWake - hrtime(true)), 1000));
            }
        }

        return [$results, $failures];
    }

    /**
     * Wait before the next look at a running statement.
     *
     * Inside a task of run() this lets the other tasks proceed for that long;
     * anywhere else it is usleep().
     */
    public static function pause(int $microseconds): void
    {
        $fiber = self::$wakeAt === null ? null : Fiber::getCurrent();

        if ($fiber !== null && isset(self::$wakeAt[$fiber])) {
            self::$wakeAt[$fiber] = hrtime(true) + $microseconds * 1000;
            Fiber::suspend();

            return;
        }

        if ($microseconds > 0) {
            usleep($microseconds);
        }
    }
}
