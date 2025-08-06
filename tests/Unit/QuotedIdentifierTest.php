<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use LaravelSnowflakeApi\SnowflakeApiConnection;
use LaravelSnowflakeApi\Flavours\Snowflake\Grammars\QueryGrammar;
use Illuminate\Database\Query\Processors\Processor;

class QuotedIdentifierTest extends TestCase
{
    /**
     * Test that the wrapColumn method handles quotes correctly for PIVOT columns
     */
    public function testWrapColumnWithQuotes()
    {
        // Get a reflection of the protected wrapColumn method
        $grammar = new QueryGrammar();
        $method = new \ReflectionMethod(QueryGrammar::class, 'wrapColumn');
        $method->setAccessible(true);
        
        // Test with a standard column name
        $result = $method->invoke($grammar, 'regular_column');
        $this->assertEquals('regular_column', $result);
        
        // Test with a column name having quotes in it
        $result = $method->invoke($grammar, "\"'column_with_quotes'\"");
        
        // Print result for debugging
        echo "Column wrapping test: " . $result . PHP_EOL;
        
        // We expect the method to preserve the quotes
        $this->assertStringContainsString("'column_with_quotes'", $result);
        
        // The key finding: the double quotes are removed but the single quotes remain
        // This affects how we need to reference these columns in ARRAY_AGG
        
        // Test with a real-world example from our PIVOT query
        $pivotColumn = "\"'9e90t3nOfBJS3oQuFn7MzI3v0G1s'\"";
        $wrappedPivotColumn = $method->invoke($grammar, $pivotColumn);
        echo "PIVOT column wrapping: " . $wrappedPivotColumn . PHP_EOL;
        
        // We expect the method to preserve the single quotes, which means when we reference this
        // column in the outer query's ARRAY_AGG, we need to include the single quotes in the identifier
        $this->assertEquals("'9e90t3nOfBJS3oQuFn7MzI3v0G1s'", $wrappedPivotColumn);
        
        // Test how column reference would be incorrect if we don't use the single quotes
        $incorrectReference = "\"9e90t3nOfBJS3oQuFn7MzI3v0G1s\""; 
        $wrappedIncorrect = $method->invoke($grammar, $incorrectReference);
        echo "Incorrect column reference: " . $wrappedIncorrect . PHP_EOL;
        
        // This shows how using the incorrect reference would lead to a mismatch
        $this->assertNotEquals($wrappedPivotColumn, $wrappedIncorrect);
    }
    
    /**
     * Document the solution to the PIVOT query issue
     */
    public function testPivotQuerySolution()
    {
        // The issue: When PIVOT creates columns with values from IN ('val1', 'val2'), 
        // the column names include the single quotes: 'val1', 'val2'
        
        // Incorrect reference in ARRAY_AGG would be:
        // ARRAY_AGG("val1") - This won't match the actual column name from PIVOT
        
        // Correct reference in ARRAY_AGG should be:
        // ARRAY_AGG("'val1'") - Notice the single quotes are part of the column identifier
        
        // For our specific query, the fix involves changing:
        // FROM: ARRAY_AGG("9e90t3nOfBJS3oQuFn7MzI3v0G1s")
        // TO:   ARRAY_AGG("'9e90t3nOfBJS3oQuFn7MzI3v0G1s'")
        
        // And similarly for all other PIVOT-generated columns
        
        $this->assertTrue(true, "This test documents the solution");
    }
} 