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

    public function test_trailing_whitespace_after_an_alias_is_not_silently_dropped(): void
    {
        // PCRE's $ matches before a trailing newline; using it here would emit
        // `X as "Foo"` and lose the newline, so flag-on output would differ from
        // flag-off by more than the quoting.
        $query = $this->query()->from('orders')->select(["MPLUS_DEMO_MAPPING as Demo\n"]);

        $this->assertSame("select MPLUS_DEMO_MAPPING as Demo\n from ORDERS", $query->toSql());
    }

    public function test_alias_extraction_does_not_backtrack_catastrophically(): void
    {
        $grammar = $this->grammar(true);
        $method = new \ReflectionMethod(QueryGrammar::class, 'extractTrailingAlias');
        $method->setAccessible(true);

        $pathological = str_repeat('a as ', 5000).'!';

        $start = hrtime(true);
        $result = $method->invoke($grammar, $pathological);
        $elapsedMs = (hrtime(true) - $start) / 1e6;

        $this->assertNull($result);
        $this->assertLessThan(250, $elapsedMs, 'extractTrailingAlias regex backtracks superlinearly');
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

    public function test_alias_references_are_not_quoted_so_aliases_must_be_upper_case(): void
    {
        // The grammar cannot tell an alias reference from a column reference, so
        // group by / having / order by emit the name unquoted. A mixed-case alias
        // therefore does NOT round-trip to its own reference: `order by Total`
        // folds to TOTAL while the alias is exactly "Total". Callers must spell
        // aliases UPPER CASE for any alias they intend to reference.
        $query = $this->query()
            ->from('orders')
            ->select(['SUM(x) as Total'])
            ->groupBy('a')
            ->orderBy('Total');

        $this->assertSame(
            'select SUM(x) as "Total" from ORDERS group by a order by Total asc',
            $query->toSql()
        );
    }

    public function test_sub_select_aliases_are_not_quoted(): void
    {
        // selectSub routes its alias through wrap() rather than the select-column
        // path, so it is NOT quoted. Documented, not fixed: wrap() cannot
        // distinguish an alias from a column without breaking plain columns.
        $query = $this->query()
            ->from('orders')
            ->selectSub(function ($query): void {
                $query->from('order_lines')->selectRaw('count(*)');
            }, 'MyCount');

        $this->assertStringContainsString('as MyCount', $query->toSql());
        $this->assertStringNotContainsString('as "MyCount"', $query->toSql());
    }

    public function test_unicode_aliases_are_quoted_like_ascii_ones(): void
    {
        $query = $this->query()->from('orders')->select(['MPLUS_DEMO_MAPPING as Démo']);

        $this->assertSame('select MPLUS_DEMO_MAPPING as "Démo" from ORDERS', $query->toSql());
    }

    public function test_a_trailing_space_after_the_alias_leaves_the_column_untouched(): void
    {
        $query = $this->query()->from('orders')->select(['MPLUS_DEMO_MAPPING as Demo ']);

        // Fail-safe: unmatched input keeps today's exact output rather than
        // guessing where the alias ends.
        $this->assertSame('select MPLUS_DEMO_MAPPING as Demo  from ORDERS', $query->toSql());
    }

    public function test_insert_and_upsert_column_names_are_never_treated_as_aliases(): void
    {
        // columnize() is shared with DML; only compileColumns() does alias
        // quoting, so a column literally named "x as y" stays a column name.
        $grammar = $this->grammar(true);
        $builder = $this->query()->from('orders');

        $sql = $grammar->compileInsert($builder, [['id' => 1, 'x as y' => 2]]);

        $this->assertStringContainsString('(id, x as y)', $sql);
        $this->assertStringNotContainsString('as "y"', $sql);
    }

    public function test_group_by_column_lists_are_not_alias_quoted(): void
    {
        $query = $this->query()->from('orders')->select(['A as B'])->groupBy('C as D');

        $this->assertSame(
            'select A as "B" from ORDERS group by C as D',
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
        $connection->shouldReceive('getDatabaseName')->andReturn('SFDVDH')->byDefault();
        $connection->shouldReceive('getConfig')->with('quote_identifiers')->andReturn($quoteIdentifiers)->byDefault();

        return $connection;
    }
}
