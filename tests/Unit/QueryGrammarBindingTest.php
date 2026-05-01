<?php

namespace Tests\Unit;

use Illuminate\Database\ConnectionInterface;
use Illuminate\Database\Query\Builder;
use Illuminate\Database\Query\Processors\Processor;
use LaravelSnowflakeApi\Flavours\Snowflake\Grammars\QueryGrammar;
use Mockery;
use Tests\TestCase;

class QueryGrammarBindingTest extends TestCase
{
    protected function tearDown(): void
    {
        Mockery::close();
        parent::tearDown();
    }

    public function test_basic_where_clauses_keep_placeholders_so_where_raw_bindings_stay_aligned(): void
    {
        $query = $this->query()
            ->from('order_detail')
            ->where('ORDER_NUMBER', '1471386')
            ->whereRaw('LOWER(STATION_CALL_LETTERS) = ?', ['kscr'])
            ->where('AIR_DATE', '>=', '2026-05-01');

        $this->assertSame(
            'select * from ORDER_DETAIL where ORDER_NUMBER = ? and LOWER(STATION_CALL_LETTERS) = ? and AIR_DATE >= ?',
            $query->toSql()
        );
        $this->assertSame(['1471386', 'kscr', '2026-05-01'], $query->getBindings());
    }

    public function test_where_raw_before_basic_where_keeps_binding_order(): void
    {
        $query = $this->query()
            ->from('order_detail')
            ->whereRaw('LOWER(STATION_CALL_LETTERS) = ?', ['kscr'])
            ->where('ORDER_NUMBER', '1471386');

        $this->assertSame(
            'select * from ORDER_DETAIL where LOWER(STATION_CALL_LETTERS) = ? and ORDER_NUMBER = ?',
            $query->toSql()
        );
        $this->assertSame(['kscr', '1471386'], $query->getBindings());
    }

    public function test_where_in_uses_placeholders_so_later_raw_bindings_are_not_shifted(): void
    {
        $query = $this->query()
            ->from('order_detail')
            ->whereIn('ORDER_NUMBER', ['1471386', '1471387'])
            ->whereRaw('LOWER(STATION_CALL_LETTERS) = ?', ['kscr']);

        $this->assertSame(
            'select * from ORDER_DETAIL where ORDER_NUMBER in (?, ?) and LOWER(STATION_CALL_LETTERS) = ?',
            $query->toSql()
        );
        $this->assertSame(['1471386', '1471387', 'kscr'], $query->getBindings());
    }

    private function query(): Builder
    {
        $connection = Mockery::mock(ConnectionInterface::class);
        $connection->shouldReceive('getName')->andReturn('snowflake_api')->byDefault();

        return new Builder($connection, new QueryGrammar, new Processor);
    }
}
