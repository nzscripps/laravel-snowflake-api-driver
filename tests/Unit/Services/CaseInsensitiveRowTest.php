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
        $old = unserialize($this->legacyPayload(
            'a:2:{s:12:"ORDER_NUMBER";i:1471386;s:4:"DEMO";s:6:"A25-54";}',
            'a:2:{s:12:"order_number";s:12:"ORDER_NUMBER";s:4:"demo";s:4:"DEMO";}',
        ));

        $this->assertInstanceOf(CaseInsensitiveRow::class, $old);
        $this->assertSame(['ORDER_NUMBER' => 1471386, 'DEMO' => 'A25-54'], (array) $old);
        $this->assertSame(1471386, $old->Order_Number);
    }

    public function test_serializes_in_the_0_12_envelope_so_older_processes_can_read_it(): void
    {
        // During a rolling deploy or a rollback a 0.12 process reads rows this
        // version cached. 0.12 has no __unserialize(): it maps the payload onto
        // its private `data` and `keyMap` arrays, so the bytes must match.
        $row = new CaseInsensitiveRow(['ID' => 7, 'NAME' => 'a']);

        $this->assertSame(
            $this->legacyPayload(
                'a:2:{s:2:"ID";i:7;s:4:"NAME";s:1:"a";}',
                'a:2:{s:2:"id";s:2:"ID";s:4:"name";s:4:"NAME";}',
            ),
            serialize($row),
        );
    }

    public function test_a_legacy_payload_keeps_the_casing_its_reads_resolved_to(): void
    {
        // 0.12 row built from FOO and Foo, then written through FOO again:
        // its keyMap sent every other casing to FOO.
        $old = unserialize($this->legacyPayload(
            'a:2:{s:3:"FOO";i:3;s:3:"Foo";i:2;}',
            'a:1:{s:3:"foo";s:3:"FOO";}',
        ));

        // Precedence is carried by property order, so FOO moves last.
        $this->assertEquals(['FOO' => 3, 'Foo' => 2], (array) $old);
        $this->assertSame(3, $old->foo);
        $this->assertSame(3, unserialize(serialize($old))->foo);
    }

    public function test_an_array_access_write_redirects_other_casings_to_it(): void
    {
        $row = new CaseInsensitiveRow(['FOO' => 1, 'Foo' => 2]);
        $this->assertSame(2, $row->foo);   // newest added twin

        $row['FOO'] = 3;

        $this->assertSame(3, $row->foo);
        $this->assertSame(3, $row['fOO']);
    }

    public function test_an_array_access_write_redirects_before_any_read(): void
    {
        $row = new CaseInsensitiveRow(['FOO' => 1, 'Foo' => 2]);

        $row['FOO'] = 3;

        $this->assertSame(3, $row->foo);
    }

    public function test_twin_precedence_survives_unrelated_unsets_and_clones(): void
    {
        $row = new CaseInsensitiveRow(['FOO' => 1, 'Foo' => 2, 'BAR' => 3]);
        $row->foo;
        $row['FOO'] = 4;

        unset($row->bar);
        $copy = clone $row;

        $this->assertSame(4, $row->foo);
        $this->assertSame(4, $copy->foo);
        $this->assertSame(4, unserialize(serialize($row))->foo);
    }

    public function test_serializes_only_live_casings_after_an_exact_name_unset(): void
    {
        $row = new CaseInsensitiveRow(['FOO' => 1, 'Foo' => 2]);
        $row->foo;   // resolves to Foo

        unset($row->Foo);   // exact name: bypasses __unset
        $payload = serialize($row);   // before any read could repair the map

        $this->assertSame(
            $this->legacyPayload('a:1:{s:3:"FOO";i:1;}', 'a:1:{s:3:"foo";s:3:"FOO";}'),
            $payload,
        );
        $this->assertSame(1, $row->foo);
    }

    public function test_a_direct_write_to_an_existing_twin_updates_it_in_place(): void
    {
        // A public property write never reaches PHP magic, so it cannot move
        // other casings (0.12 did). Documented: only ArrayAccess writes do.
        $row = new CaseInsensitiveRow(['FOO' => 1, 'Foo' => 2]);
        $row->foo;

        $row->FOO = 4;

        $this->assertSame(4, $row->FOO);
        $this->assertSame(2, $row->foo);
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
     * A row exactly as 0.12 serialized it (no __serialize, private arrays).
     */
    private function legacyPayload(string $data, string $keyMap): string
    {
        $class = CaseInsensitiveRow::class;
        $dataKey = "\0{$class}\0data";
        $keyMapKey = "\0{$class}\0keyMap";

        return sprintf(
            'O:%d:"%s":2:{s:%d:"%s";%ss:%d:"%s";%s}',
            strlen($class), $class, strlen($dataKey), $dataKey, $data, strlen($keyMapKey), $keyMapKey, $keyMap,
        );
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
