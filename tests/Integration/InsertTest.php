<?php

namespace Tests\Integration;

use Tests\TestCase;
use Mockery;
use LaravelSnowflakeApi\SnowflakeApiConnection;
use LaravelSnowflakeApi\Services\SnowflakeService;
use LaravelSnowflakeApi\Services\Result;
use LaravelSnowflakeApi\Flavours\Snowflake\Grammars\QueryGrammar;
use LaravelSnowflakeApi\Flavours\Snowflake\Grammars\SchemaGrammar;
use Illuminate\Database\Query\Processors\Processor;
use Illuminate\Support\Facades\DB;
use Illuminate\Database\Query\Builder;

class InsertTest extends TestCase
{
    protected $connection;
    protected $mockSnowflakeService;

    protected function setUp(): void
    {
        parent::setUp();
        
        $this->mockSnowflakeService = Mockery::mock(SnowflakeService::class);
        $this->mockSnowflakeService->shouldReceive('testConnection')->andReturn(true);
        
        // Create a mock connection
        $this->connection = Mockery::mock(SnowflakeApiConnection::class);
        
        // Always use our custom mock for the snowflake service
        $this->connection->shouldReceive('getSnowflakeService')->andReturn($this->mockSnowflakeService);
        
        // Set up the query grammar
        $grammar = new QueryGrammar();
        $this->connection->shouldReceive('getQueryGrammar')->andReturn($grammar);
        
        // Set up the post processor
        $processor = new Processor();
        $this->connection->shouldReceive('getPostProcessor')->andReturn($processor);
        
        // These methods are called during query execution
        $this->connection->shouldReceive('getTablePrefix')->andReturn('');
        $this->connection->shouldReceive('pretending')->andReturn(false);
        $this->connection->shouldReceive('getName')->andReturn('snowflake_api');
    }
    
    /**
     * Test SQL generation directly with the grammar class
     */
    public function testCompileInsertWithColumns()
    {
        // Data to insert
        $data = [
            ['id' => 1, 'name' => 'Test 1', 'value' => 100],
            ['id' => 2, 'name' => 'Test 2', 'value' => 200],
            ['id' => 3, 'name' => 'Test 3', 'value' => 300],
        ];
        
        // Set up the query manually
        $query = new class($this->connection) extends Builder {
            public function __construct($connection) 
            {
                parent::__construct($connection);
                $this->from = 'test_table';
            }
        };
        
        // Create a grammar directly
        $grammar = new QueryGrammar();
        
        // Compile the insert statement
        $sql = $grammar->compileInsert($query, $data);
        
        // Verify the SQL has the correct format with column names
        $this->assertStringContainsString('insert into TEST_TABLE (id, name, value) values', $sql);
        $this->assertStringContainsString("(1, 'Test 1', 100)", $sql);
        $this->assertStringContainsString("(2, 'Test 2', 200)", $sql);
        $this->assertStringContainsString("(3, 'Test 3', 300)", $sql);
    }
    
    /**
     * Test SQL generation for chunked inserts
     */
    public function testChunkedInsertSqlGeneration()
    {
        // Data to insert
        $data = [
            ['id' => 1, 'name' => 'Test 1'],
            ['id' => 2, 'name' => 'Test 2'],
        ];
        
        // Split into chunks
        $chunks = array_chunk($data, 1);
        
        // Generate SQL for each chunk
        $sqlStatements = [];
        foreach ($chunks as $chunk) {
            // Create a grammar directly
            $grammar = new QueryGrammar();
            
            // Set up the query manually
            $query = new class($this->connection) extends Builder {
                public function __construct($connection) 
                {
                    parent::__construct($connection);
                    $this->from = 'test_table';
                }
            };
            
            // Generate the SQL
            $sqlStatements[] = $grammar->compileInsert($query, $chunk);
        }
        
        // Verify correct SQL was generated
        $this->assertCount(2, $sqlStatements);
        $this->assertStringContainsString('insert into TEST_TABLE (id, name) values', $sqlStatements[0]);
        $this->assertStringContainsString("(1, 'Test 1')", $sqlStatements[0]);
        $this->assertStringContainsString('insert into TEST_TABLE (id, name) values', $sqlStatements[1]);
        $this->assertStringContainsString("(2, 'Test 2')", $sqlStatements[1]);
    }
    
    protected function tearDown(): void
    {
        Mockery::close();
        parent::tearDown();
    }
} 