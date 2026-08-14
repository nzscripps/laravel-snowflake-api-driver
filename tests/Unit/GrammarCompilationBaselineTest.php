<?php

namespace Tests\Unit;

use Illuminate\Database\Connection;
use Illuminate\Database\Query\Builder;
use Illuminate\Database\Query\Processors\Processor;
use LaravelSnowflakeApi\Flavours\Snowflake\Grammars\QueryGrammar;
use Mockery;
use Tests\TestCase;

/**
 * Byte-for-byte compilation baseline captured from tag v0.10.2 (commit 492c6a0).
 *
 * The `quote_identifiers` flag is opt-in: with it OFF the emitted SQL must stay
 * identical to 0.10.2 for every consumer that has not flipped it. Any diff here
 * is a breaking change for existing deployments, not a test that needs updating.
 *
 * @see QuoteIdentifiersGrammarTest for the flag-on behaviour.
 */
class GrammarCompilationBaselineTest extends TestCase
{
    protected function tearDown(): void
    {
        Mockery::close();
        parent::tearDown();
    }

    /**
     * @return array<string, array{0: callable, 1: string}>
     */
    public static function baselineCases(): array
    {
        return [
            'select alias, mixed case' => [
                fn (Builder $q) => $q->from('vp_cpln_enriched')->select(['ORDER_NUMBER', 'MPLUS_DEMO_MAPPING as Demo']),
                'select ORDER_NUMBER, MPLUS_DEMO_MAPPING as Demo from VP_CPLN_ENRICHED',
            ],
            'select alias, lower case' => [
                fn (Builder $q) => $q->from('vp_cpln_enriched')->select(['MPLUS_DEMO_MAPPING as demo']),
                'select MPLUS_DEMO_MAPPING as demo from VP_CPLN_ENRICHED',
            ],
            'qualified column' => [
                fn (Builder $q) => $q->from('orders')->select(['orders.order_number']),
                'select orders.order_number from ORDERS',
            ],
            'table alias' => [
                fn (Builder $q) => $q->from('orders as o')->select(['o.order_number']),
                "select o.order_number from ORDERS as 'O'",
            ],
            'join alias' => [
                fn (Builder $q) => $q->from('orders')->join('order_lines as ol', 'ol.order_id', '=', 'orders.id'),
                "select * from ORDERS inner join ORDER_LINES as 'OL' on OL.order_id = ORDERS.id",
            ],
            'raw select alias' => [
                fn (Builder $q) => $q->from('orders')->selectRaw('SUM(DELIVERED) as Delivered_Impressions'),
                'select SUM(DELIVERED) as Delivered_Impressions from ORDERS',
            ],
            'predicates, groups and orders' => [
                fn (Builder $q) => $q->from('orders')->select(['A as B'])
                    ->where('Mixed_Col', 1)->groupBy('Mixed_Col')->orderBy('Mixed_Col'),
                'select A as B from ORDERS where Mixed_Col = ? group by Mixed_Col order by Mixed_Col asc',
            ],
            'pivot style quoted column' => [
                fn (Builder $q) => $q->from('orders')->select(['"\'9e90t3nOfBJS3oQuFn7MzI3v0G1s\'"']),
                "select '9e90t3nOfBJS3oQuFn7MzI3v0G1s' from ORDERS",
            ],
        ];
    }

    /**
     * @dataProvider baselineCases
     */
    public function test_compilation_is_unchanged_when_the_flag_is_off(callable $build, string $expected): void
    {
        $this->withEnv('SNOWFLAKE_QUOTE_IDENTIFIERS', null, function () use ($build, $expected) {
            $this->assertSame($expected, $build($this->query(null))->toSql());
        });
    }

    /**
     * @dataProvider baselineCases
     */
    public function test_compilation_is_unchanged_when_the_flag_is_explicitly_false(callable $build, string $expected): void
    {
        $this->assertSame($expected, $build($this->query(false))->toSql());
    }

    /**
     * @param  mixed  $quoteIdentifiers
     */
    private function query($quoteIdentifiers): Builder
    {
        $connection = Mockery::mock(Connection::class);
        $connection->shouldReceive('getName')->andReturn('snowflake_api')->byDefault();
        $connection->shouldReceive('getTablePrefix')->andReturn('')->byDefault();
        $connection->shouldReceive('getConfig')->with('quote_identifiers')->andReturn($quoteIdentifiers)->byDefault();

        $grammar = new QueryGrammar;
        $grammar->setConnection($connection);

        return new Builder($connection, $grammar, new Processor);
    }
}
