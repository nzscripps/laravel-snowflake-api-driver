<?php

namespace Tests\Integration;

use Illuminate\Database\Query\Builder;
use LaravelSnowflakeApi\Flavours\Snowflake\Grammars\QueryGrammar;
use LaravelSnowflakeApi\Services\Result;
use LaravelSnowflakeApi\SnowflakeApiConnection;
use Mockery;
use Tests\TestCase;

class ColumnMappingInsertTest extends TestCase
{
    protected $connection;

    protected $grammar;

    protected function setUp(): void
    {
        parent::setUp();

        // Create the grammar directly for testing
        $this->grammar = new QueryGrammar;

        // Create a mock connection
        $this->connection = Mockery::mock(SnowflakeApiConnection::class)
            ->makePartial() // Restore makePartial
            ->shouldAllowMockingProtectedMethods();

        // Set up mocks for methods needed
        $this->connection->shouldReceive('getTablePrefix')->andReturn('');
        $this->connection->shouldReceive('getName')->andReturn('snowflake_api');

        // Mock getQueryGrammar to return the manually created instance
        $this->connection->shouldReceive('getQueryGrammar')->andReturn($this->grammar);

        // No need to manually set connection here if getQueryGrammar is mocked
        // $this->grammar->setConnection($this->connection);
    }

    /**
     * Test column determination with explicit columns
     */
    public function test_determine_columns_with_explicit_columns()
    {
        // Set up a query with explicit columns
        $query = new Builder($this->connection);
        $query->from = 'test_table';
        $query->columns = ['id', 'name', 'value'];

        // Get a reflection method to test the protected method
        $method = new \ReflectionMethod($this->grammar, 'determineInsertColumns');
        $method->setAccessible(true);

        // Test with a row that has different keys
        $row = ['user_id' => 1, 'username' => 'test', 'active' => true];

        // The method should use the explicit columns, not the row keys
        $columns = $method->invoke($this->grammar, $query, $row);

        $this->assertEquals(['id', 'name', 'value'], $columns,
            'Should use explicit columns from query even if row has different keys');
    }

    /**
     * Test column determination with associative arrays
     */
    public function test_determine_columns_with_associative_array()
    {
        // Set up a query without explicit columns
        $query = new Builder($this->connection);
        $query->from = 'test_table';

        // Get a reflection method to test the protected method
        $method = new \ReflectionMethod($this->grammar, 'determineInsertColumns');
        $method->setAccessible(true);

        // Test with an associative array
        $row = ['id' => 1, 'name' => 'test', 'value' => 100];

        // The method should use the row keys as columns
        $columns = $method->invoke($this->grammar, $query, $row);

        $this->assertEquals(['id', 'name', 'value'], $columns,
            'Should use associative array keys as column names');
    }

    /**
     * Test column determination with numeric arrays
     */
    public function test_determine_columns_with_numeric_array()
    {
        // Set up a query without explicit columns
        $query = new Builder($this->connection);
        $query->from = 'test_table';

        // Get a reflection method to test the protected method
        $method = new \ReflectionMethod($this->grammar, 'determineInsertColumns');
        $method->setAccessible(true);

        // Test with a numeric array
        $row = [1, 'test', 100];

        // The method should create generic column names
        $columns = $method->invoke($this->grammar, $query, $row);

        $this->assertEquals(['col_0', 'col_1', 'col_2'], $columns,
            'Should create generic column names for numeric arrays');
    }

    /**
     * Test the full insert compilation with explicit columns
     */
    public function test_compile_insert_with_explicit_columns()
    {
        // Set up a query with explicit columns
        $query = new Builder($this->connection);
        $query->from = 'test_table';
        $query->columns = ['campaign_id', 'break_id', 'callsign'];

        // Create test data with numeric keys (like the error case)
        $values = [
            [0 => 1094615, 1 => 'A44716D5-23C9-45CA-BB6F-D85A466860A0-0', 2 => 'WTKR'],
            [0 => 1094646, 1 => '54F5A58F-E962-4EC4-B5DD-EA9BE2D0F920-0', 2 => 'WTKR'],
        ];

        // Compile the insert statement
        $sql = $this->grammar->compileInsert($query, $values);

        // Verify the SQL uses the explicit column names
        $this->assertStringContainsString(
            'insert into TEST_TABLE (campaign_id, break_id, callsign) values',
            $sql,
            'Should use explicit column names in SQL'
        );

        // Verify values are included correctly
        $this->assertStringContainsString(
            "(1094615, 'A44716D5-23C9-45CA-BB6F-D85A466860A0-0', 'WTKR')",
            $sql,
            'Should format first row values correctly'
        );

        $this->assertStringContainsString(
            "(1094646, '54F5A58F-E962-4EC4-B5DD-EA9BE2D0F920-0', 'WTKR')",
            $sql,
            'Should format second row values correctly'
        );
    }

    /**
     * Test the connection's insertWithColumns method
     */
    public function test_insert_with_columns()
    {
        // Track SQL for validation
        $capturedSql = '';

        // Mock parts of the connection for testing
        $this->connection->shouldReceive('table')->andReturnUsing(function ($table) {
            $query = new Builder($this->connection);
            $query->from = $table;

            return $query;
        });

        $this->connection->shouldReceive('insert')->andReturnUsing(function ($query, $bindings = []) use (&$capturedSql) {
            $capturedSql = $query;

            return true;
        });

        // Execute the method with test data
        $columns = ['campaign_id', 'break_id', 'source_callsign'];
        $values = [
            [0 => 1094615, 1 => 'A44716D5-23C9-45CA-BB6F-D85A466860A0-0', 2 => 'WTKR'],
            [0 => 1094646, 1 => '54F5A58F-E962-4EC4-B5DD-EA9BE2D0F920-0', 2 => 'WTKR'],
        ];

        $result = $this->connection->insertWithColumns('BREAK_AVERAGE_CANDIDATES', $columns, $values);

        // Verify the result
        $this->assertTrue($result, 'Insert should succeed');

        // Check if SQL was captured
        $this->assertNotEmpty($capturedSql, 'SQL should be generated');
    }

    protected function tearDown(): void
    {
        Mockery::close();
        parent::tearDown();
    }
}
