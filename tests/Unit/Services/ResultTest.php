<?php

namespace Tests\Unit\Services;

use Illuminate\Support\Collection;
use Illuminate\Support\Facades\Log;
use LaravelSnowflakeApi\Services\Result;
use Mockery;
use Tests\TestCase;

class ResultTest extends TestCase
{
    private $result;

    protected function setUp(): void
    {
        parent::setUp();

        // Mock Log facade to avoid actual logging during tests
        Log::shouldReceive('debug')->byDefault();

        $this->result = new Result;

        // Use reflection to disable debug logging
        $this->setPrivateProperty($this->result, 'isDebugEnabled', false);
    }

    protected function tearDown(): void
    {
        Mockery::close();
        parent::tearDown();
    }

    /** @test */
    public function it_converts_data_types_correctly()
    {
        // Arrange
        $this->result->setFields([
            ['name' => 'bool_col', 'type' => 'BOOLEAN'],
            ['name' => 'date_col', 'type' => 'DATE'],
            ['name' => 'num_col', 'type' => 'INTEGER'],
        ]);

        $this->result->setData([
            ['true', '2023-01-01', '123'],
        ]);

        // Act
        $converted = $this->result->toArray();

        // Assert
        $this->assertIsBool($converted[0]['bool_col']);
        $this->assertIsString($converted[0]['date_col']);
        $this->assertEquals('2023-01-01', $converted[0]['date_col']);
        $this->assertIsInt($converted[0]['num_col']);
    }

    /** @test */
    public function it_handles_pagination_data()
    {
        // Arrange
        $this->result->setData([['col1' => 'page1']]);

        // Act
        $this->result->addPageData([['col1' => 'page2']]);
        $data = $this->result->getData();

        // Assert
        $this->assertCount(2, $data);
        $this->assertEquals('page1', $data[0]['col1']);
        $this->assertEquals('page2', $data[1]['col1']);
    }

    /** @test */
    public function it_sets_and_gets_properties_correctly()
    {
        // Arrange & Act
        $this->result->setId('test-id');
        $this->result->setExecuted(true);
        $this->result->setTotal(100);
        $this->result->setPage(1);
        $this->result->setPageTotal(5);
        $this->result->setTimestamp('2023-01-01');

        // Assert
        $this->assertEquals('test-id', $this->result->getId());
        $this->assertTrue($this->result->isExecuted());
        $this->assertEquals(100, $this->result->getTotal());
        $this->assertEquals(1, $this->result->getPage());
        $this->assertEquals(5, $this->result->getPageTotal());
        $this->assertEquals('2023-01-01', $this->result->getTimestamp());
    }

    /** @test */
    public function it_converts_to_collection()
    {
        // Arrange
        $this->result->setFields([
            ['name' => 'id', 'type' => 'INTEGER'],
            ['name' => 'name', 'type' => 'VARCHAR'],
        ]);

        $this->result->setData([
            ['1', 'test1'],
            ['2', 'test2'],
        ]);

        // Act
        $collection = $this->result->toCollection();

        // Assert
        $this->assertInstanceOf(Collection::class, $collection);
        $this->assertCount(2, $collection);
        $this->assertEquals('test1', $collection[0]['name']);
        $this->assertEquals(2, $collection[1]['id']);
    }

    /** @test */
    public function it_handles_empty_data_set()
    {
        // Arrange
        $this->result->setFields([]);
        $this->result->setData([]);

        // Act
        $array = $this->result->toArray();
        $collection = $this->result->toCollection();
        $count = $this->result->count();

        // Assert
        $this->assertEmpty($array);
        $this->assertInstanceOf(Collection::class, $collection);
        $this->assertEmpty($collection);
        $this->assertEquals(0, $count);
    }
}
