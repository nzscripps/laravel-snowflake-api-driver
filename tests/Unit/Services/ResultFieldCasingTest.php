<?php

namespace Tests\Unit\Services;

use Illuminate\Support\Facades\Log;
use LaravelSnowflakeApi\Services\CaseInsensitiveRow;
use LaravelSnowflakeApi\Services\Result;
use Mockery;
use Tests\TestCase;

/**
 * Hydration is casing-transparent: whatever `resultSetMetaData.rowType` names a
 * field, that exact string becomes the PHP array key.
 *
 * This is the contract the `quote_identifiers` flag depends on. The driver does
 * not upper-case anything on the way back — an UPPERCASE key means Snowflake
 * folded an unquoted alias, and quoting the alias is what makes `Mixed_Case`
 * survive. These fixtures exist because ResultTest only ever used lower-case
 * field names, so nothing pinned that behaviour.
 */
class ResultFieldCasingTest extends TestCase
{
    protected function setUp(): void
    {
        parent::setUp();

        Log::shouldReceive('debug')->byDefault();
    }

    protected function tearDown(): void
    {
        Mockery::close();
        parent::tearDown();
    }

    public function test_uppercase_field_names_are_hydrated_verbatim(): void
    {
        $result = $this->hydratedResult([
            ['name' => 'ORDER_NUMBER', 'type' => 'TEXT'],
            ['name' => 'MPLUS_DEMO_MAPPING', 'type' => 'TEXT'],
        ], [['1471386', 'A25-54']]);

        $rows = $result->toArray();

        $this->assertSame(['ORDER_NUMBER', 'MPLUS_DEMO_MAPPING'], array_keys($rows[0]));
        $this->assertSame('A25-54', $rows[0]['MPLUS_DEMO_MAPPING']);
        $this->assertArrayNotHasKey('mplus_demo_mapping', $rows[0]);
    }

    public function test_mixed_case_field_names_are_hydrated_verbatim(): void
    {
        $result = $this->hydratedResult([
            ['name' => 'Demo', 'type' => 'TEXT'],
            ['name' => 'Delivered_Impressions', 'type' => 'FIXED'],
        ], [['A25-54', '1200']]);

        $rows = $result->toArray();

        $this->assertSame(['Demo', 'Delivered_Impressions'], array_keys($rows[0]));
        $this->assertSame('A25-54', $rows[0]['Demo']);
        $this->assertSame(1200, $rows[0]['Delivered_Impressions']);
        $this->assertArrayNotHasKey('DEMO', $rows[0]);
    }

    public function test_mixed_case_survives_the_row_mapping_and_streaming_paths(): void
    {
        $result = $this->hydratedResult([
            ['name' => 'Demo', 'type' => 'TEXT'],
        ], [['A25-54'], ['A18-49']]);

        $this->assertSame(['A25-54', 'A18-49'], $result->mapRows(static fn (array $row) => $row['Demo']));
        $this->assertSame(['Demo' => 'A25-54'], $result->rowToArray(['A25-54']));

        $streamed = iterator_to_array($result->rowsFrom([['A34-64']]), false);
        $this->assertSame(['Demo' => 'A34-64'], $streamed[0]);
    }

    public function test_case_insensitive_row_reads_either_casing_but_reports_raw_keys(): void
    {
        $row = new CaseInsensitiveRow(['Demo' => 'A25-54', 'ORDER_NUMBER' => '1471386']);

        // Reads are patched at the Connection layer, so consumers can use any casing.
        $this->assertSame('A25-54', $row->Demo);
        $this->assertSame('A25-54', $row->demo);
        $this->assertSame('A25-54', $row['DEMO']);
        $this->assertSame('1471386', $row->order_number);

        // Serialisation is NOT patched — this is the surface that changes for
        // consumers when a connection flips `quote_identifiers` on.
        $this->assertSame(['Demo' => 'A25-54', 'ORDER_NUMBER' => '1471386'], $row->toArray());
        $this->assertSame(['Demo' => 'A25-54', 'ORDER_NUMBER' => '1471386'], $row->jsonSerialize());
    }

    /**
     * @param  array<int, array{name: string, type: string}>  $fields
     * @param  array<int, array<int, string>>  $data
     */
    private function hydratedResult(array $fields, array $data): Result
    {
        $result = new Result;
        $this->setPrivateProperty($result, 'isDebugEnabled', false);
        $result->setFields($fields);
        $result->setData($data);

        return $result;
    }
}
