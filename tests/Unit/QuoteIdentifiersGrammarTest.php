<?php

namespace Tests\Unit;

use Illuminate\Database\Connection;
use Illuminate\Database\Query\Builder;
use Illuminate\Database\Query\Processors\Processor;
use LaravelSnowflakeApi\Flavours\Snowflake\Grammars\QueryGrammar;
use Mockery;
use Tests\TestCase;

/**
 * Behaviour of the opt-in `quote_identifiers` connection flag.
 *
 * With the flag ON the grammar emits SELECT column aliases and table aliases as
 * double-quoted Snowflake identifiers, so mixed-case aliases survive into
 * `resultSetMetaData.rowType` instead of being folded to UPPER CASE, and table
 * aliases stop being emitted as single-quoted string literals.
 *
 * Column REFERENCES are deliberately left unquoted in both flag states: Snowflake
 * stores unquoted identifiers upper-cased, so quoting `order_number` as
 * `"order_number"` would stop resolving. Only alias emission changes.
 */
class QuoteIdentifiersGrammarTest extends TestCase
{
    protected function tearDown(): void
    {
        Mockery::close();
        parent::tearDown();
    }

    public function test_select_alias_keeps_its_case_when_flag_is_on(): void
    {
        $query = $this->query()
            ->from('vp_cpln_enriched')
            ->select(['ORDER_NUMBER', 'MPLUS_DEMO_MAPPING as Demo']);

        $this->assertSame(
            'select ORDER_NUMBER, MPLUS_DEMO_MAPPING as "Demo" from VP_CPLN_ENRICHED',
            $query->toSql()
        );
    }

    public function test_lower_case_select_alias_is_quoted_when_flag_is_on(): void
    {
        $query = $this->query()
            ->from('vp_cpln_enriched')
            ->select(['MPLUS_DEMO_MAPPING as demo']);

        $this->assertSame(
            'select MPLUS_DEMO_MAPPING as "demo" from VP_CPLN_ENRICHED',
            $query->toSql()
        );
    }

    public function test_expression_alias_in_a_select_string_is_quoted_without_touching_the_expression(): void
    {
        $query = $this->query()
            ->from('orders')
            ->select(['SUM(t.DELIVERED_IMPRESSIONS) as Delivered_Impressions']);

        $this->assertSame(
            'select SUM(t.DELIVERED_IMPRESSIONS) as "Delivered_Impressions" from ORDERS',
            $query->toSql()
        );
    }

    public function test_table_alias_is_a_double_quoted_identifier_not_a_string_literal_when_flag_is_on(): void
    {
        $query = $this->query()->from('orders as o')->select(['o.order_number']);

        // The alias is emitted upper-cased (resolveTableName) and quoted, while the
        // unquoted reference `o.` folds to O — so the two still resolve to each other.
        $this->assertSame(
            'select o.order_number from ORDERS as "O"',
            $query->toSql()
        );
    }

    public function test_join_alias_is_a_double_quoted_identifier_when_flag_is_on(): void
    {
        $query = $this->query()
            ->from('orders')
            ->join('order_lines as ol', 'ol.order_id', '=', 'orders.id');

        $this->assertSame(
            'select * from ORDERS inner join ORDER_LINES as "OL" on OL.order_id = ORDERS.id',
            $query->toSql()
        );
    }

    public function test_explicitly_quoted_alias_is_honoured_rather_than_stripped(): void
    {
        $query = $this->query()
            ->from('orders')
            ->select(['MPLUS_DEMO_MAPPING as "Demo"']);

        $this->assertSame(
            'select MPLUS_DEMO_MAPPING as "Demo" from ORDERS',
            $query->toSql()
        );
    }

    public function test_embedded_double_quotes_in_an_alias_are_escaped_by_doubling(): void
    {
        $grammar = $this->grammar(true);
        $method = new \ReflectionMethod(QueryGrammar::class, 'wrapValue');
        $method->setAccessible(true);

        $this->assertSame('"we""ird"', $method->invoke($grammar, 'we"ird'));
        $this->assertSame('*', $method->invoke($grammar, '*'));
    }

    public function test_qualified_column_references_are_untouched_when_flag_is_on(): void
    {
        $query = $this->query()->from('orders')->select(['orders.order_number']);

        $this->assertSame('select orders.order_number from ORDERS', $query->toSql());
    }

    public function test_pivot_style_quoted_columns_are_untouched_when_flag_is_on(): void
    {
        $query = $this->query()->from('orders')->select(['"\'9e90t3nOfBJS3oQuFn7MzI3v0G1s\'"']);

        $this->assertSame(
            "select '9e90t3nOfBJS3oQuFn7MzI3v0G1s' from ORDERS",
            $query->toSql()
        );
    }

    public function test_raw_expressions_are_never_rewritten_by_the_flag(): void
    {
        $query = $this->query()
            ->from('orders')
            ->selectRaw('SUM(DELIVERED) as Delivered_Impressions');

        $this->assertSame(
            'select SUM(DELIVERED) as Delivered_Impressions from ORDERS',
            $query->toSql()
        );
    }

    public function test_a_trailing_as_that_is_not_a_simple_alias_is_left_alone(): void
    {
        $query = $this->query()
            ->from('orders')
            ->select(["IFF(STATUS = 'a as b', 1, 0)"]);

        $this->assertSame(
            "select IFF(STATUS = 'a as b', 1, 0) from ORDERS",
            $query->toSql()
        );
    }

    public function test_predicates_groups_and_orders_are_untouched_when_flag_is_on(): void
    {
        $query = $this->query()
            ->from('orders')
            ->select(['A as B'])
            ->where('Mixed_Col', 1)
            ->groupBy('Mixed_Col')
            ->orderBy('Mixed_Col');

        $this->assertSame(
            'select A as "B" from ORDERS where Mixed_Col = ? group by Mixed_Col order by Mixed_Col asc',
            $query->toSql()
        );
    }

    public function test_flag_defaults_to_off_when_nothing_is_configured(): void
    {
        $this->withEnv('SNOWFLAKE_QUOTE_IDENTIFIERS', null, function () {
            $this->assertFalse((new QueryGrammar)->quotesIdentifiers());
            $this->assertFalse($this->grammar(null)->quotesIdentifiers());
        });
    }

    public function test_flag_is_read_from_the_connection_config(): void
    {
        $this->assertTrue($this->grammar(true)->quotesIdentifiers());
        $this->assertFalse($this->grammar(false)->quotesIdentifiers());
        $this->assertTrue($this->grammar('true')->quotesIdentifiers());
        $this->assertTrue($this->grammar('1')->quotesIdentifiers());
        $this->assertFalse($this->grammar('false')->quotesIdentifiers());
    }

    public function test_explicit_setter_overrides_the_connection_config(): void
    {
        $this->assertTrue($this->grammar(false)->setQuoteIdentifiers(true)->quotesIdentifiers());
        $this->assertFalse($this->grammar(true)->setQuoteIdentifiers(false)->quotesIdentifiers());
    }

    public function test_env_is_the_fallback_when_the_connection_has_no_setting(): void
    {
        $this->withEnv('SNOWFLAKE_QUOTE_IDENTIFIERS', 'true', function () {
            $this->assertTrue((new QueryGrammar)->quotesIdentifiers());
            $this->assertTrue($this->grammar(null)->quotesIdentifiers());
        });
    }

    public function test_connection_config_wins_over_env(): void
    {
        $this->withEnv('SNOWFLAKE_QUOTE_IDENTIFIERS', 'true', function () {
            $this->assertFalse($this->grammar(false)->quotesIdentifiers());
        });
    }

    /**
     * @param  mixed  $quoteIdentifiers  value returned by the connection for `quote_identifiers`
     */
    private function grammar($quoteIdentifiers): QueryGrammar
    {
        $grammar = new QueryGrammar;
        $grammar->setConnection($this->connection($quoteIdentifiers));

        return $grammar;
    }

    private function query(): Builder
    {
        return new Builder($this->connection(true), $this->grammar(true), new Processor);
    }

    /**
     * @param  mixed  $quoteIdentifiers
     */
    private function connection($quoteIdentifiers = true): Connection
    {
        $connection = Mockery::mock(Connection::class);
        $connection->shouldReceive('getName')->andReturn('snowflake_api')->byDefault();
        $connection->shouldReceive('getTablePrefix')->andReturn('')->byDefault();
        $connection->shouldReceive('getConfig')->with('quote_identifiers')->andReturn($quoteIdentifiers)->byDefault();

        return $connection;
    }
}
