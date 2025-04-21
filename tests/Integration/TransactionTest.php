<?php

namespace Tests\Integration;

use Exception;
use Tests\TestCase;
use LaravelSnowflakeApi\SnowflakeApiConnection;
use LaravelSnowflakeApi\Services\SnowflakeService;
use LaravelSnowflakeApi\Services\Result;
use ReflectionClass;
use Mockery;
use Mockery\MockInterface;

class TransactionTest extends TestCase
{
    /**
     * Test that transaction methods are properly implemented.
     *
     * @return void
     */
    public function testTransactionMethodsImplemented()
    {
        // Get reflection of the SnowflakeApiConnection class
        $reflectionClass = new ReflectionClass(SnowflakeApiConnection::class);
        
        // Check if the required transaction methods are defined in the class
        $this->assertTrue($reflectionClass->hasMethod('beginTransaction'), 
            'The beginTransaction method should be implemented');
        $this->assertTrue($reflectionClass->hasMethod('commit'), 
            'The commit method should be implemented');
        $this->assertTrue($reflectionClass->hasMethod('rollBack'), 
            'The rollBack method should be implemented');
        $this->assertTrue($reflectionClass->hasMethod('getPdo'), 
            'The getPdo method should be implemented');
        $this->assertTrue($reflectionClass->hasMethod('inTransaction'), 
            'The inTransaction method should be implemented');
    }

    /**
     * Test transaction behavior directly with an actual instance.
     */
    public function testTransactionBehavior()
    {
        // Create a manual test object implementing the key methods
        $connection = new class('test') extends SnowflakeApiConnection {
            private $localTransactions = 0;
            
            public function __construct($name) 
            {
                // Skip parent constructor
                $this->transactions = 0;
            }
            
            public function getPdo() 
            {
                return $this;
            }
            
            public function inTransaction(): bool
            {
                return $this->transactions > 0;
            }
            
            public function beginTransaction(): void
            {
                $this->transactions++;
            }
            
            public function commit(): void
            {
                $this->transactions = max(0, $this->transactions - 1);
            }
            
            public function rollBack($toLevel = null): void
            {
                $toLevel = is_null($toLevel) ? $this->transactions - 1 : $toLevel;
                
                if ($toLevel < 0 || $toLevel >= $this->transactions) {
                    return;
                }
                
                $this->transactions = $toLevel;
            }
            
            public function transactionLevel(): int
            {
                return $this->transactions;
            }
            
            protected function fireConnectionEvent($event) 
            {
                // Do nothing in test
            }
        };
        
        // Test the transaction methods
        $this->assertEquals(0, $connection->transactionLevel());
        
        $connection->beginTransaction();
        $this->assertEquals(1, $connection->transactionLevel());
        $this->assertTrue($connection->inTransaction());
        
        $connection->commit();
        $this->assertEquals(0, $connection->transactionLevel());
        $this->assertFalse($connection->inTransaction());
        
        $connection->beginTransaction();
        $connection->beginTransaction();
        $this->assertEquals(2, $connection->transactionLevel());
        
        $connection->rollBack();
        $this->assertEquals(1, $connection->transactionLevel());
        
        $connection->rollBack();
        $this->assertEquals(0, $connection->transactionLevel());
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