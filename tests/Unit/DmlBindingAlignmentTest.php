<?php

declare(strict_types=1);

namespace Tests\Unit;

use Illuminate\Database\Query\Builder;
use Illuminate\Database\Query\Expression;
use Illuminate\Database\Query\Processors\Processor;
use LaravelSnowflakeApi\Flavours\Snowflake\Grammars\QueryGrammar;
use LaravelSnowflakeApi\SnowflakeApiConnection;
use ReflectionClass;
use Tests\TestCase;

/**
 * Update and delete compile their target table, joins, SET values and WHERE
 * clause, in that order, so their bindings must follow that order and must not
 * carry the order, group, having or union bindings a builder may hold: those
 * clauses never reach the SQL text.
 */
class DmlBindingAlignmentTest extends TestCase
{
    private SnowflakeApiConnection $connection;

    protected function setUp(): void
    {
        parent::setUp();

        $this->connection = (new ReflectionClass(SnowflakeApiConnection::class))->newInstanceWithoutConstructor();
        $this->connection->setQueryGrammar(new QueryGrammar($this->connection));
    }

    private function query(): Builder
    {
        return new Builder($this->connection, $this->connection->getQueryGrammar(), new Processor);
    }

    private function prepare(string $sql, array $bindings): string
    {
        return $this->invokePrivateMethod($this->connection, 'prepareQuery', $sql, $bindings);
    }

    public function test_an_update_ignores_bound_ordering_it_does_not_compile(): void
    {
        $query = $this->query()->from('a')->where('id', 42)
            ->orderByRaw('case when priority = ? then 0 else 1 end', [9]);
        $grammar = $this->connection->getQueryGrammar();
        $values = ['status' => 'done'];

        $sql = $grammar->compileUpdate($query, $values);
        $bindings = $grammar->prepareBindingsForUpdate($query->getRawBindings(), $values);

        $this->assertSame(['done', 42], $bindings);
        $this->assertSame("update A set status = 'done' where id = 42", $this->prepare($sql, $bindings));
    }

    public function test_a_delete_ignores_bound_ordering_it_does_not_compile(): void
    {
        $query = $this->query()->from('a')->where('id', 42)
            ->orderByRaw('case when priority = ? then 0 else 1 end', [9]);
        $grammar = $this->connection->getQueryGrammar();

        $sql = $grammar->compileDelete($query);
        $bindings = $grammar->prepareBindingsForDelete($query->getRawBindings());

        $this->assertSame([42], $bindings);
        $this->assertSame('delete from A where id = 42', $this->prepare($sql, $bindings));
    }

    private function prepareUpdate(Builder $query, array $values): string
    {
        $grammar = $this->connection->getQueryGrammar();

        return $this->prepare(
            $grammar->compileUpdate($query, $values),
            $query->cleanBindings($grammar->prepareBindingsForUpdate($query->getRawBindings(), $values)),
        );
    }

    public function test_a_delete_keeps_the_binding_of_a_bound_target(): void
    {
        $query = $this->query()->fromRaw('IDENTIFIER(?)', ['a'])->where('id', 42);
        $grammar = $this->connection->getQueryGrammar();

        $this->assertSame(
            "delete from IDENTIFIER('a') where id = 42",
            $this->prepare($grammar->compileDelete($query), $grammar->prepareBindingsForDelete($query->getRawBindings())),
        );
    }

    public function test_an_update_binds_a_bound_target_before_its_set_values(): void
    {
        $query = $this->query()->fromRaw('IDENTIFIER(?)', ['a'])->where('id', 42);

        $this->assertSame(
            "update IDENTIFIER('a') set status = 'done' where id = 42",
            $this->prepareUpdate($query, ['status' => 'done']),
        );
        $this->assertSame(
            "update IDENTIFIER('a') set status = 'done' where id = 42",
            $this->prepareUpdate($query, ['status' => new Expression("'done'")]),
        );
    }
}
