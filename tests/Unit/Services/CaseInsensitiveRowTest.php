<?php

namespace Tests\Unit\Services;

use Illuminate\Database\Connection;
use Illuminate\Database\Query\Processors\Processor;
use LaravelSnowflakeApi\Flavours\Snowflake\Grammars\QueryGrammar;
use LaravelSnowflakeApi\Flavours\Snowflake\SnowflakeQueryBuilder;
use LaravelSnowflakeApi\Services\CaseInsensitiveRow;
use Mockery;
use Tests\TestCase;

/**
 * A result row must behave like the stdClass row Laravel code is written for:
 * columns visible to casts, get_object_vars(), property_exists() and foreach,
 * plus case-insensitive reads for the casings Snowflake folds away.
 *
 * Until 0.13 the columns sat in a private array, so `(array) $row` produced two
 * mangled keys, property_exists() was always false, and Laravel's
 * Builder::value() returned the whole private array instead of the column.
 * Consumers read nothing and their stdClass-based tests stayed green.
 */
class CaseInsensitiveRowTest extends TestCase
{
    protected function tearDown(): void
    {
        Mockery::close();
        parent::tearDown();
    }

    public function test_columns_are_visible_to_casts_and_object_introspection(): void
    {
        $row = new CaseInsensitiveRow(['ORDER_NUMBER' => 1471386, 'SPOT_COUNT' => 2, 'DEMO' => null]);
        $expected = ['ORDER_NUMBER' => 1471386, 'SPOT_COUNT' => 2, 'DEMO' => null];

        $iterated = [];
        foreach ($row as $column => $value) {
            $iterated[$column] = $value;
        }

        $this->assertInstanceOf(\stdClass::class, $row);
        $this->assertSame($expected, (array) $row);
        $this->assertSame($expected, get_object_vars($row));
        $this->assertSame($expected, $iterated);
        $this->assertSame($expected, $row->toArray());
        $this->assertSame($expected, $row->jsonSerialize());
        $this->assertSame('{"ORDER_NUMBER":1471386,"SPOT_COUNT":2,"DEMO":null}', json_encode($row));
        $this->assertTrue(property_exists($row, 'SPOT_COUNT'));
        $this->assertTrue(property_exists($row, 'DEMO'));
        $this->assertFalse(property_exists($row, 'MISSING'));
    }

    public function test_any_casing_reads_the_column(): void
    {
        $row = new CaseInsensitiveRow(['ORDER_NUMBER' => 1471386]);

        $this->assertSame(1471386, $row->ORDER_NUMBER);
        $this->assertSame(1471386, $row->Order_Number);
        $this->assertSame(1471386, $row->order_number);
        $this->assertSame(1471386, $row['Order_Number']);
        $this->assertTrue(isset($row->order_number));
        $this->assertTrue(isset($row['ORDER_NUMBER']));
        $this->assertNull($row->Missing);
        $this->assertNull($row['missing']);
        $this->assertFalse(isset($row->missing));
    }

    public function test_isset_follows_stdclass_semantics_for_a_null_column_in_every_casing(): void
    {
        $row = new CaseInsensitiveRow(['DEMO' => null]);

        $this->assertFalse(isset($row->DEMO));
        $this->assertFalse(isset($row->Demo));
        $this->assertFalse(isset($row['demo']));
        $this->assertSame('fallback', $row->Demo ?? 'fallback');
        $this->assertTrue(empty($row->demo));
        // Selection is asked with the column's own name.
        $this->assertTrue(property_exists($row, 'DEMO'));
        $this->assertArrayHasKey('DEMO', $row->toArray());
    }

    public function test_writing_another_casing_adds_a_twin_and_reads_resolve_to_the_newest(): void
    {
        $row = new CaseInsensitiveRow(['DELIVERED_TO_DATE_IMP_RTG' => 1.0]);
        $row->delivered_to_date_imp_rtg;   // build the casing map before the write

        $row->Delivered_to_date_IMP_RTG = 4.0;

        $this->assertSame(1.0, $row->DELIVERED_TO_DATE_IMP_RTG);
        $this->assertSame(4.0, $row->Delivered_to_date_IMP_RTG);
        $this->assertSame(4.0, $row->delivered_to_date_imp_rtg);
        $this->assertSame(
            ['DELIVERED_TO_DATE_IMP_RTG' => 1.0, 'Delivered_to_date_IMP_RTG' => 4.0],
            (array) $row,
        );
    }

    public function test_writing_the_exact_name_updates_the_column_in_place(): void
    {
        $row = new CaseInsensitiveRow(['SPOT_COUNT' => 1]);

        $row->SPOT_COUNT = 5;
        $row['Spot_Length'] = 30;

        $this->assertSame(['SPOT_COUNT' => 5, 'Spot_Length' => 30], (array) $row);
        $this->assertSame(5, $row->spot_count);
        $this->assertSame(30, $row['SPOT_LENGTH']);
    }

    public function test_unset_in_any_casing_removes_the_column(): void
    {
        $row = new CaseInsensitiveRow(['HAS_BREAK_AVERAGE' => 0, 'SPOT_COUNT' => 2, 'DEMO' => 'A25-54']);
        $row->has_break_average;   // build the casing map first

        unset($row->Has_Break_Average);
        unset($row->SPOT_COUNT);   // exact name: PHP removes it without __unset
        unset($row['demo']);

        $this->assertSame([], (array) $row);
        $this->assertNull($row->spot_count);
        $this->assertFalse(isset($row->Spot_Count));
    }

    public function test_a_clone_is_independent_and_reads_any_casing(): void
    {
        $row = new CaseInsensitiveRow(['HAS_GROUP_EXCEPTION' => 1, 'SPOT_COUNT' => 2]);
        $row->spot_count;

        $copy = clone $row;
        unset($copy->HAS_GROUP_EXCEPTION);
        $copy->Spot_Count = 9;

        $this->assertSame(['HAS_GROUP_EXCEPTION' => 1, 'SPOT_COUNT' => 2], (array) $row);
        $this->assertSame(2, $row->spot_count);
        $this->assertSame(9, $copy->spot_count);
        $this->assertNull($copy->has_group_exception);
    }

    public function test_serialization_round_trips_and_restores_rows_cached_before_0_13(): void
    {
        $row = new CaseInsensitiveRow(['ORDER_NUMBER' => 1471386, 'DEMO' => 'A25-54']);

        $restored = unserialize(serialize($row));

        $this->assertInstanceOf(CaseInsensitiveRow::class, $restored);
        $this->assertSame(['ORDER_NUMBER' => 1471386, 'DEMO' => 'A25-54'], (array) $restored);
        $this->assertSame('A25-54', $restored->demo);

        // What 0.12 and earlier wrote: private `data` and `keyMap` arrays.
        $class = CaseInsensitiveRow::class;
        $data = "\0{$class}\0data";
        $keyMap = "\0{$class}\0keyMap";
        $legacy = sprintf(
            'O:%d:"%s":2:{s:%d:"%s";a:2:{s:12:"ORDER_NUMBER";i:1471386;s:4:"DEMO";s:6:"A25-54";}s:%d:"%s";a:2:{s:12:"order_number";s:12:"ORDER_NUMBER";s:4:"demo";s:4:"DEMO";}}',
            strlen($class), $class, strlen($data), $data, strlen($keyMap), $keyMap,
        );

        $old = unserialize($legacy);

        $this->assertInstanceOf(CaseInsensitiveRow::class, $old);
        $this->assertSame(['ORDER_NUMBER' => 1471386, 'DEMO' => 'A25-54'], (array) $old);
        $this->assertSame(1471386, $old->Order_Number);
    }

    public function test_builder_value_returns_the_column_not_the_row(): void
    {
        $query = $this->snowflakeQuery([new CaseInsensitiveRow(['ROW_COUNT' => 42])]);

        $this->assertSame(42, $query->from('VP_CPLN_ORDERS')->value('ROW_COUNT'));
    }

    public function test_builder_first_and_pluck_read_driver_rows(): void
    {
        $rows = [
            new CaseInsensitiveRow(['ORDER_NUMBER' => 1001, 'STATION' => 'WEWS']),
            new CaseInsensitiveRow(['ORDER_NUMBER' => 1002, 'STATION' => 'KNXV']),
        ];

        $first = $this->snowflakeQuery($rows)->from('VP_CPLN_ORDERS')->first();
        $stations = $this->snowflakeQuery($rows)->from('VP_CPLN_ORDERS')->pluck('Station', 'Order_Number');

        $this->assertSame(1001, $first->Order_Number);
        $this->assertSame(['ORDER_NUMBER' => 1001, 'STATION' => 'WEWS'], (array) $first);
        $this->assertSame([1001 => 'WEWS', 1002 => 'KNXV'], $stations->all());
    }

    /**
     * @param  list<CaseInsensitiveRow>  $rows  what SnowflakeApiConnection::select() returns
     */
    private function snowflakeQuery(array $rows): SnowflakeQueryBuilder
    {
        $connection = Mockery::mock(Connection::class);
        $connection->shouldReceive('getName')->andReturn('snowflake_api')->byDefault();
        $connection->shouldReceive('getTablePrefix')->andReturn('')->byDefault();
        $connection->shouldReceive('getConfig')->andReturn(null)->byDefault();
        $connection->shouldReceive('select')->andReturn($rows);

        $grammar = new QueryGrammar;
        $grammar->setConnection($connection);

        return new SnowflakeQueryBuilder($connection, $grammar, new Processor);
    }
}
