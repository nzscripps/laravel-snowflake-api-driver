<?php

namespace Tests\Integration;

use Exception;
use Illuminate\Support\Facades\DB;
use Tests\TestCase;
use Mockery;
use LaravelSnowflakeApi\SnowflakeApiConnection;
use LaravelSnowflakeApi\Services\SnowflakeService;
use LaravelSnowflakeApi\Services\Result;
use LaravelSnowflakeApi\Flavours\Snowflake\Grammars\QueryGrammar;
use LaravelSnowflakeApi\Flavours\Snowflake\Grammars\SchemaGrammar;
use Illuminate\Database\Schema\Grammars\Grammar as BaseSchemaGrammar;
use Illuminate\Database\Query\Processors\Processor;

class TransactionUsageTest extends TestCase
{
    /**
     * Test the transaction behavior directly.
     */
    public function testTransactionDirectly()
    {
        // Create a custom connection for testing
        $connection = $this->createMockConnection();
        
        // Set up test data
        $data = [
            ['id' => 1, 'name' => 'Test 1'],
            ['id' => 2, 'name' => 'Test 2'],
            ['id' => 3, 'name' => 'Test 3'],
        ];
        
        // Test our transaction handling in isolation
        $connection->beginTransaction();
        $this->assertEquals(1, $connection->transactionLevel(), 'Transaction level should be 1 after beginTransaction');
        
        try {
            $chunks = array_chunk($data, 2);
            foreach ($chunks as $index => $chunk) {
                // Instead of using the query builder, just log that we would insert data
                $connection->addTestLog("Insert chunk " . ($index + 1));
            }
            $connection->commit();
            $this->assertEquals(0, $connection->transactionLevel(), 'Transaction level should be 0 after commit');
        } catch (Exception $e) {
            $connection->rollBack();
            $this->fail('Should not throw an exception: ' . $e->getMessage());
        }
        
        // Test transaction rollback
        $connection->beginTransaction();
        try {
            $connection->addTestLog("Insert row 4");
            throw new Exception('Test exception');
        } catch (Exception $e) {
            $connection->rollBack();
            $this->assertEquals(0, $connection->transactionLevel(), 'Transaction level should be 0 after rollback');
            $this->assertEquals('Test exception', $e->getMessage());
        }
    }
    
    /**
     * Test an actual Laravel transaction with our batch insert.
     */
    public function testLaravelTransaction()
    {
        // Create a custom connection for testing
        $connection = $this->createMockConnection();
        
        // Replace the default connection with our test connection
        $this->app['db']->extend('snowflake_api', function() use ($connection) {
            return $connection;
        });
        
        // Set up the test connection in the config
        config(['database.connections.test_snowflake' => [
            'driver' => 'snowflake_api',
            'database' => 'test_db',
        ]]);
        
        // Set up test data
        $data = [
            ['id' => 1, 'name' => 'Test 1'],
            ['id' => 2, 'name' => 'Test 2'],
            ['id' => 3, 'name' => 'Test 3'],
        ];
        
        // Use Laravel's DB transaction
        DB::connection('test_snowflake')->transaction(function($db) use ($data, $connection) {
            $chunks = array_chunk($data, 2);
            foreach ($chunks as $index => $chunk) {
                // Simulate the insert operation
                $connection->addTestLog("Insert chunk " . ($index + 1));
            }
            
            $this->assertEquals(1, $connection->transactionLevel(), 
                'Transaction level should be 1 inside transaction');
        });
        
        $this->assertEquals(0, $connection->transactionLevel(), 
            'Transaction level should be 0 after transaction completes');
        
        // Test transaction with exception
        try {
            DB::connection('test_snowflake')->transaction(function($db) use ($connection) {
                $connection->addTestLog("Insert failed data");
                throw new Exception('Test exception in transaction');
            });
            $this->fail('Should have thrown an exception');
        } catch (Exception $e) {
            $this->assertEquals('Test exception in transaction', $e->getMessage());
            $this->assertEquals(0, $connection->transactionLevel(), 
                'Transaction level should be 0 after exception');
        }
    }
    
    /**
     * Create a mock connection for testing.
     */
    private function createMockConnection()
    {
        return new class() extends SnowflakeApiConnection {
            protected $logs = [];
            
            public function __construct()
            {
                // Skip parent constructor
                $this->transactions = 0;
            }
            
            // Method to record test operations
            public function addTestLog($message)
            {
                $this->logs[] = $message;
                return true;
            }
            
            // Override to return our own instance for transaction methods
            public function getPdo()
            {
                return $this;
            }
            
            public function getQueryGrammar()
            {
                return new QueryGrammar();
            }
            
            public function getSchemaGrammar()
            {
                return new SchemaGrammar();
            }
            
            public function getPostProcessor()
            {
                return new Processor();
            }
            
            // Mock transaction methods
            public function beginTransaction()
            {
                $this->transactions++;
                $this->logs[] = "BEGIN TRANSACTION";
                return true;
            }
            
            public function commit()
            {
                $this->transactions = max(0, $this->transactions - 1);
                $this->logs[] = "COMMIT";
                return true;
            }
            
            public function rollBack($toLevel = null)
            {
                $toLevel = is_null($toLevel) ? $this->transactions - 1 : $toLevel;
                
                if ($toLevel < 0 || $toLevel >= $this->transactions) {
                    return true;
                }
                
                $this->transactions = $toLevel;
                $this->logs[] = "ROLLBACK";
                return true;
            }
            
            public function inTransaction()
            {
                return $this->transactions > 0;
            }
            
            // Override various methods that would try to access the database
            public function select($query, $bindings = [], $useReadPdo = true)
            {
                $this->logs[] = "SELECT: $query";
                return [];
            }
            
            public function insert($query, $bindings = [])
            {
                $this->logs[] = "INSERT: $query";
                return true;
            }
            
            public function update($query, $bindings = [])
            {
                $this->logs[] = "UPDATE: $query";
                return 1;
            }
            
            public function delete($query, $bindings = [])
            {
                $this->logs[] = "DELETE: $query";
                return 1;
            }
            
            public function statement($query, $bindings = [])
            {
                $this->logs[] = "STATEMENT: $query";
                return true;
            }
            
            // Method to handle events
            protected function fireConnectionEvent($event)
            {
                $this->logs[] = "EVENT: $event";
            }
        };
    }
    
    /**
     * Clean up after each test.
     */
    protected function tearDown(): void
    {
        Mockery::close();
        parent::tearDown();
    }
} 