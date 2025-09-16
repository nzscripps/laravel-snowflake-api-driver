<?php

namespace Tests\Integration;

use Illuminate\Database\Query\Builder;
use Illuminate\Database\Query\Processors\Processor;
use LaravelSnowflakeApi\Flavours\Snowflake\Grammars\QueryGrammar;
use LaravelSnowflakeApi\Services\SnowflakeService;
use LaravelSnowflakeApi\SnowflakeApiConnection;
use Mockery;
use Tests\TestCase;

class NumericColumnsInsertTest extends TestCase
{
    protected $connection;

    protected $mockSnowflakeService;

    protected $grammar;

    protected function setUp(): void
    {
        parent::setUp();

        $this->mockSnowflakeService = Mockery::mock(SnowflakeService::class);
        $this->mockSnowflakeService->shouldReceive('testConnection')->andReturn(true);

        // Create a mock connection
        $this->connection = Mockery::mock(SnowflakeApiConnection::class)->makePartial()
            ->shouldAllowMockingProtectedMethods();

        // Create the grammar directly
        $this->grammar = new QueryGrammar;

        // Always use our custom mock for the snowflake service
        $this->connection->shouldReceive('getSnowflakeService')->andReturn($this->mockSnowflakeService);

        // Set up the post processor
        $processor = new Processor; // Assuming base Processor is fine
        $this->connection->shouldReceive('getPostProcessor')->andReturn($processor);

        // Explicitly mock getQueryGrammar to return our instance
        $this->connection->shouldReceive('getQueryGrammar')->andReturn($this->grammar);

        // These methods are called during query execution or by grammar
        $this->connection->shouldReceive('getTablePrefix')->andReturn('');
        $this->connection->shouldReceive('pretending')->andReturn(false);
        $this->connection->shouldReceive('getName')->andReturn('snowflake_api');
        $this->connection->shouldReceive('parameter')->andReturnUsing(function ($value) { // Mock parameter method used by grammar
            if (is_null($value)) {
                return 'NULL';
            }
            if (is_bool($value)) {
                return $value ? 'TRUE' : 'FALSE';
            }
            if (is_numeric($value)) {
                return (string) $value;
            }

            return "'".str_replace("'", "''", $value)."'";
        });
    }

    /**
     * Test compiling inserts with numeric array keys
     */
    public function test_compile_insert_with_numeric_keys()
    {
        // Data with numeric keys
        $data = [
            [0 => 1, 1 => 'Test 1', 2 => 100],
            [0 => 2, 1 => 'Test 2', 2 => 200],
            [0 => 3, 1 => 'Test 3', 2 => 300],
        ];

        // Column names
        $columns = ['id', 'name', 'value'];

        // Set up the query manually
        $query = new class($this->connection) extends Builder
        {
            public function __construct($connection)
            {
                parent::__construct($connection);
                $this->from = 'test_table';
                $this->columns = ['id', 'name', 'value'];
            }
        };

        // Compile the insert statement
        $sql = $this->grammar->compileInsert($query, $data);

        // Verify the SQL has the correct format with column names
        $this->assertStringContainsString('insert into TEST_TABLE (id, name, value) values', $sql);
        $this->assertStringContainsString("(1, 'Test 1', 100)", $sql);
        $this->assertStringContainsString("(2, 'Test 2', 200)", $sql);
        $this->assertStringContainsString("(3, 'Test 3', 300)", $sql);
    }

    /**
     * Test compiling inserts with numeric array keys and no columns
     */
    public function test_compile_insert_with_numeric_keys_no_columns()
    {
        // Data with numeric keys
        $data = [
            [0 => 1, 1 => 'Test 1', 2 => 100],
            [0 => 2, 1 => 'Test 2', 2 => 200],
            [0 => 3, 1 => 'Test 3', 2 => 300],
        ];

        // Set up the query manually
        $query = new class($this->connection) extends Builder
        {
            public function __construct($connection)
            {
                parent::__construct($connection);
                $this->from = 'test_table';
            }
        };

        // Compile the insert statement
        $sql = $this->grammar->compileInsert($query, $data);

        // Verify the SQL has generated column names
        $this->assertStringContainsString('insert into TEST_TABLE (col_0, col_1, col_2) values', $sql);
        $this->assertStringContainsString("(1, 'Test 1', 100)", $sql);
        $this->assertStringContainsString("(2, 'Test 2', 200)", $sql);
        $this->assertStringContainsString("(3, 'Test 3', 300)", $sql);
    }

    /**
     * Test using insertWithColumns method
     */
    public function test_insert_with_columns_method()
    {
        // Capture the SQL for validation
        $capturedSql = '';

        // Mock the insert method to capture the query
        $this->connection->shouldReceive('insert')->andReturnUsing(function ($query, $bindings = []) use (&$capturedSql) {
            $capturedSql = (string) $query;

            return true;
        });

        // Data with numeric keys - simulating data from the error case
        $data = [
            [0 => 1094615, 1 => 'A44716D5-23C9-45CA-BB6F-D85A466860A0-0', 2 => 'WTKR'],
            [0 => 1094615, 1 => 'D650AAE5-C3FF-4746-847E-CF75EF731FC0-0', 2 => 'WTKR'],
        ];

        // Columns from the error case
        $columns = [
            'campaign_id', 'break_id', 'source_callsign',
        ];

        // Allow table method to work
        $this->connection->shouldReceive('table')->andReturnUsing(function ($table) {
            $query = new Builder($this->connection);
            $query->from = $table;

            return $query;
        });

        // Call our new insertWithColumns method
        $result = $this->connection->insertWithColumns('BREAK_AVERAGE_CANDIDATES', $columns, $data);

        // Verify the method returned success
        $this->assertTrue($result);

        // Verify the SQL looks correct
        $this->assertNotEmpty($capturedSql, 'SQL should be captured');
        $this->assertStringContainsString('insert into BREAK_AVERAGE_CANDIDATES (campaign_id, break_id, source_callsign)', $capturedSql);
    }

    protected function tearDown(): void
    {
        Mockery::close();
        parent::tearDown();
    }
}
