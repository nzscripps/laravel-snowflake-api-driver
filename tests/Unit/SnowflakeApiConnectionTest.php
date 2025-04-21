<?php

namespace Tests\Unit;

use Tests\TestCase;
use LaravelSnowflakeApi\SnowflakeApiConnection;
use LaravelSnowflakeApi\Flavours\Snowflake\Grammars\QueryGrammar;
use LaravelSnowflakeApi\Flavours\Snowflake\Grammars\SchemaGrammar;
use LaravelSnowflakeApi\Services\SnowflakeService;
use Mockery;

class SnowflakeApiConnectionTest extends TestCase
{
    protected function tearDown(): void
    {
        Mockery::close();
        parent::tearDown();
    }

    public function testGetDefaultQueryGrammarInLaravel12()
    {
        // Create a minimal mock connection
        $connection = new class extends SnowflakeApiConnection {
            public function __construct()
            {
                // Override constructor to avoid actual connection setup
            }

            // Make the protected method public for testing
            public function getDefaultQueryGrammarTest()
            {
                return $this->getDefaultQueryGrammar();
            }
        };

        // Manually invoke getDefaultQueryGrammar using our test accessor
        $grammar = $connection->getDefaultQueryGrammarTest();
        
        // Verify we got a properly configured grammar back
        $this->assertInstanceOf(QueryGrammar::class, $grammar);
        $this->assertEquals('', $grammar->getTablePrefix());
    }

    public function testGetDefaultSchemaGrammarInLaravel12()
    {
        // Create a minimal mock connection
        $connection = new class extends SnowflakeApiConnection {
            public function __construct()
            {
                // Override constructor to avoid actual connection setup
            }

            // Make the protected method public for testing
            public function getDefaultSchemaGrammarTest()
            {
                return $this->getDefaultSchemaGrammar();
            }
        };

        // Manually invoke getDefaultSchemaGrammar using our test accessor
        $grammar = $connection->getDefaultSchemaGrammarTest();
        
        // Verify we got a properly configured grammar back
        $this->assertInstanceOf(SchemaGrammar::class, $grammar);
        $this->assertEquals('', $grammar->getTablePrefix());
    }
    
    public function testWithTablePrefixOnSchemaGrammar()
    {
        // Create test prefix
        $prefix = 'test_prefix_';
        
        // Create our connection with mocked internals
        $connection = new class extends SnowflakeApiConnection {
            public function __construct()
            {
                // Override constructor to avoid actual connection setup
            }
            
            // Mock tablePrefix for testing
            public $tablePrefix = '';
        };
        
        // Create a schema grammar instance
        $grammar = new SchemaGrammar();
        
        // Apply the prefix
        $result = $connection->withTablePrefix($grammar);
        
        // Test that the grammar was returned
        $this->assertSame($grammar, $result);
        
        // Now set a prefix and test again
        $connection->tablePrefix = $prefix;
        $result = $connection->withTablePrefix($grammar);
        
        // Verify the grammar has the correct prefix
        $this->assertEquals($prefix, $grammar->getTablePrefix());
    }
} 