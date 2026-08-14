<?php

namespace Tests\Integration;

use Dotenv\Dotenv;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use Tests\TestCase;

/**
 * The one thing unit tests cannot prove: what Snowflake puts in
 * `resultSetMetaData.rowType` for an aliased column.
 *
 * With `quote_identifiers` off, an unquoted alias is folded to UPPER CASE by the
 * server. With it on, the alias is a double-quoted identifier and its case
 * survives the round trip. This test is the release gate for tag 0.11.0 — run it
 * before any consumer flips the flag.
 */
class QuoteIdentifiersMetadataTest extends TestCase
{
    private string $tableName;

    protected function setUp(): void
    {
        parent::setUp();

        if (file_exists(__DIR__.'/../../.env.testing.local')) {
            Dotenv::createImmutable(__DIR__.'/../../', '.env.testing.local')->load();
        }

        Log::shouldReceive('debug')->byDefault();
        Log::shouldReceive('error')->byDefault();
        Log::shouldReceive('info')->byDefault();

        if (empty(env('SNOWFLAKE_TEST_ACCOUNT'))) {
            $this->markTestSkipped('Snowflake test environment not configured.');

            return;
        }

        $base = config('database.connections.snowflake');
        config([
            'database.connections.snowflake_quoted' => array_merge($base, ['quote_identifiers' => true]),
            'database.connections.snowflake_unquoted' => array_merge($base, ['quote_identifiers' => false]),
        ]);

        $this->tableName = 'test_quote_identifiers_'.uniqid();
        DB::connection('snowflake_unquoted')->statement(
            "CREATE TABLE {$this->tableName} (ID NUMBER, MPLUS_DEMO_MAPPING VARCHAR)"
        );
        DB::connection('snowflake_unquoted')->table($this->tableName)->insert([
            'ID' => 1,
            'MPLUS_DEMO_MAPPING' => 'A25-54',
        ]);
    }

    protected function tearDown(): void
    {
        if (isset($this->tableName)) {
            DB::connection('snowflake_unquoted')->statement("DROP TABLE IF EXISTS {$this->tableName}");
        }

        parent::tearDown();
    }

    public function test_unquoted_alias_comes_back_upper_cased(): void
    {
        $row = DB::connection('snowflake_unquoted')
            ->table($this->tableName)
            ->select(['MPLUS_DEMO_MAPPING as Demo'])
            ->get()
            ->first();

        $this->assertSame(['DEMO' => 'A25-54'], $row->toArray());
    }

    public function test_quoted_alias_keeps_its_case_in_result_metadata(): void
    {
        $row = DB::connection('snowflake_quoted')
            ->table($this->tableName)
            ->select(['MPLUS_DEMO_MAPPING as Demo'])
            ->get()
            ->first();

        $this->assertSame(['Demo' => 'A25-54'], $row->toArray());
    }

    public function test_quoted_table_alias_is_accepted_by_snowflake(): void
    {
        $row = DB::connection('snowflake_quoted')
            ->table($this->tableName.' as t')
            ->select(['t.MPLUS_DEMO_MAPPING as Demo_Value'])
            ->get()
            ->first();

        $this->assertSame(['Demo_Value' => 'A25-54'], $row->toArray());
    }

    public function test_unaliased_columns_still_resolve_with_the_flag_on(): void
    {
        $row = DB::connection('snowflake_quoted')
            ->table($this->tableName)
            ->select(['id', 'mplus_demo_mapping'])
            ->get()
            ->first();

        $this->assertSame(['ID' => 1, 'MPLUS_DEMO_MAPPING' => 'A25-54'], $row->toArray());
    }
}
