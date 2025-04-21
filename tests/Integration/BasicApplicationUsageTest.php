<?php

namespace Tests\Integration;

use Tests\TestCase;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use LaravelSnowflakeApi\Flavours\Snowflake\Grammars\QueryGrammar;

class BasicApplicationUsageTest extends TestCase
{
    protected function setUp(): void
    {
        parent::setUp();
        
        // Mock Log facade to reduce noise in tests
        Log::shouldReceive('debug')->byDefault();
        Log::shouldReceive('error')->byDefault();
        Log::shouldReceive('info')->byDefault();
        
        // Skip if Snowflake environment isn't configured
        if (empty(env('SNOWFLAKE_TEST_ACCOUNT'))) {
            $this->markTestSkipped('Snowflake test environment not configured.');
            return;
        }
    }
    
    /**
     * Test basic connection and query execution, just like a regular application would do
     *
     * @return void
     */
    public function testBasicConnectionAndQuery()
    {
        // This is how a typical Laravel application would access the driver
        $connection = DB::connection('snowflake');
        
        // Verify we got the right connection type
        $this->assertInstanceOf(\LaravelSnowflakeApi\SnowflakeApiConnection::class, $connection);
        
        // Create a test table
        $tableName = 'test_table_' . uniqid();
        $connection->statement("CREATE TABLE {$tableName} (id NUMBER, name VARCHAR)");
        
        // Insert data using builder
        $result = $connection->table($tableName)->insert([
            'id' => 1,
            'name' => 'Test Record'
        ]);
        
        $this->assertTrue($result);
        
        // Query data using the query builder
        $records = $connection->table($tableName)->get();
        
        $this->assertCount(1, $records);
        $this->assertEquals(1, $records[0]->ID);
        $this->assertEquals('Test Record', $records[0]->NAME);
        
        // Clean up
        $connection->statement("DROP TABLE IF EXISTS {$tableName}");
    }
    
    /**
     * Test query builder functionality with where clauses 
     *
     * @return void
     */
    public function testQueryBuilderWithConditions()
    {
        // Get the connection
        $connection = DB::connection('snowflake');
        
        // Create a test table
        $tableName = 'test_table_' . uniqid();
        $connection->statement("CREATE TABLE {$tableName} (id NUMBER, name VARCHAR, active BOOLEAN)");
        
        // Insert multiple records
        $connection->table($tableName)->insert([
            ['id' => 1, 'name' => 'Record One', 'active' => true],
            ['id' => 2, 'name' => 'Record Two', 'active' => false],
            ['id' => 3, 'name' => 'Record Three', 'active' => true]
        ]);
        
        // Query with where clause
        $activeRecords = $connection->table($tableName)
            ->where('active', true)
            ->get();
            
        $this->assertCount(2, $activeRecords);
        
        // Query with complex where clause
        $filtered = $connection->table($tableName)
            ->where('id', '>', 1)
            ->where('active', true)
            ->first();
            
        $this->assertNotNull($filtered);
        $this->assertEquals(3, $filtered->ID);
        $this->assertEquals('Record Three', $filtered->NAME);
        
        // Clean up
        $connection->statement("DROP TABLE IF EXISTS {$tableName}");
    }
    
    /**
     * Test transactions as a typical application would use them
     *
     * @return void
     */
    public function testTransactions()
    {
        $connection = DB::connection('snowflake');
        
        // Create a test table
        $tableName = 'test_table_' . uniqid();
        $connection->statement("CREATE TABLE {$tableName} (id NUMBER, name VARCHAR)");
        
        // Use transaction - this is a common pattern in Laravel apps
        $connection->transaction(function($conn) use ($tableName) {
            $conn->table($tableName)->insert(['id' => 1, 'name' => 'Transaction Test']);
            $conn->table($tableName)->where('id', 1)->update(['name' => 'Updated in Transaction']);
        });
        
        // Verify transaction was committed
        $record = $connection->table($tableName)->find(1);
        $this->assertNotNull($record);
        $this->assertEquals('Updated in Transaction', $record->NAME);
        
        // Clean up
        $connection->statement("DROP TABLE IF EXISTS {$tableName}");
    }
} 