<?php

declare(strict_types=1);

namespace Tests\Unit;

use Illuminate\Database\Query\Builder;
use Illuminate\Database\Query\Processors\Processor;
use LaravelSnowflakeApi\Flavours\Snowflake\Grammars\QueryGrammar;
use LaravelSnowflakeApi\SnowflakeApiConnection;
use ReflectionClass;
use ReflectionMethod;
use Tests\TestCase;

/**
 * Laravel's Grammar::compileSelect() already appends the unions. Appending
 * them again compiles every union query twice, so its placeholders appear
 * twice while its bindings appear once.
 */
class UnionCompilationTest extends TestCase
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

    private function prepare(Builder $query): string
    {
        return (new ReflectionMethod(SnowflakeApiConnection::class, 'prepareQuery'))
            ->invoke($this->connection, $query->toSql(), $query->getBindings());
    }

    public function test_a_union_is_compiled_once(): void
    {
        $query = $this->query()->from('a')->where('x', 1)
            ->union($this->query()->from('b')->where('y', 'two'));

        $this->assertSame(
            'select * from (select * from A where x = ?) union select * from (select * from B where y = ?)',
            $query->toSql(),
        );
        $this->assertSame(
            "select * from (select * from A where x = 1) union select * from (select * from B where y = 'two')",
            $this->prepare($query),
        );
    }

    public function test_every_union_binding_is_consumed_once(): void
    {
        $query = $this->query()->from('a')->where('x', 1)
            ->union($this->query()->from('b')->where('y', 2))
            ->unionAll($this->query()->from('c')->where('z', 3))
            ->orderBy('x');

        $this->assertSame(3, substr_count($query->toSql(), '?'));
        $this->assertSame([1, 2, 3], $query->getBindings());
        $this->assertStringContainsString('WHERE Z = 3', strtoupper($this->prepare($query)));
    }
}
