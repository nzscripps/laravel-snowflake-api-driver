<?php

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use LaravelSnowflakeApi\Flavours\Snowflake\Grammars\QueryGrammar;

class PivotSqlParsingTest extends TestCase
{
    /**
     * Test that the query grammar correctly handles column names with quotes from PIVOT operations
     */
    public function testPivotColumnQuotesHandling()
    {
        $grammar = new QueryGrammar();
        
        // The query from the original issue
        $sql = "
            SELECT DISTINCT
                ARRAY_AGG(DATE_CATEGORY) within group (order by DATE_CATEGORY asc) AS DATE_CATEGORY,
                ARRAY_AGG(\"'9e90t3nOfBJS3oQuFn7MzI3v0G1s'\") within group (order by DATE_CATEGORY asc) AS \"9e90t3nOfBJS3oQuFn7MzI3v0G1s\",
                ARRAY_AGG(\"'Wzlyy2fuuDSUiTmzhqIq4dGVV1QB'\") within group (order by DATE_CATEGORY asc) AS \"Wzlyy2fuuDSUiTmzhqIq4dGVV1QB\"
            FROM (
                SELECT DISTINCT
                    DATE_CATEGORY,
                    \"'9e90t3nOfBJS3oQuFn7MzI3v0G1s'\" AS \"'9e90t3nOfBJS3oQuFn7MzI3v0G1s'\",
                    \"'Wzlyy2fuuDSUiTmzhqIq4dGVV1QB'\" AS \"'Wzlyy2fuuDSUiTmzhqIq4dGVV1QB'\"
                FROM some_data_source
            ) a
        ";
        
        // Test that the grammar can successfully parse the query
        // This shouldn't throw exceptions if the grammar handles quoted identifiers correctly
        $processedSql = $grammar->processColumns($sql);
        
        // The test passes if no exception is thrown while processing
        $this->assertNotNull($processedSql);
        
        // Check that the ARRAY_AGG with quoted column names remains intact
        $this->assertStringContainsString(
            "ARRAY_AGG(\"'9e90t3nOfBJS3oQuFn7MzI3v0G1s'\")",
            $processedSql
        );
        
        $this->assertStringContainsString(
            "ARRAY_AGG(\"'Wzlyy2fuuDSUiTmzhqIq4dGVV1QB'\")",
            $processedSql
        );
    }
    
    /**
     * Test query grammar with a simplified version of the problem query
     */
    public function testPivotQueryParsing()
    {
        $grammar = new QueryGrammar();
        
        // Simplified version of the query that reproduces the issue
        $sql = "
            WITH test_data AS (
                SELECT 1 AS ID, '9e90t3nOfBJS3oQuFn7MzI3v0G1s' AS BEACON_ID, 10 AS DIFF_FRAC_FLOOR
                UNION ALL
                SELECT 2 AS ID, 'Wzlyy2fuuDSUiTmzhqIq4dGVV1QB' AS BEACON_ID, 20 AS DIFF_FRAC_FLOOR
            )
            SELECT 
                ARRAY_AGG(ID) WITHIN GROUP (ORDER BY ID ASC) AS IDS,
                ARRAY_AGG(\"'9e90t3nOfBJS3oQuFn7MzI3v0G1s'\") WITHIN GROUP (ORDER BY ID ASC) AS \"9e90t3nOfBJS3oQuFn7MzI3v0G1s\",
                ARRAY_AGG(\"'Wzlyy2fuuDSUiTmzhqIq4dGVV1QB'\") WITHIN GROUP (ORDER BY ID ASC) AS \"Wzlyy2fuuDSUiTmzhqIq4dGVV1QB\"
            FROM (
                SELECT 
                    ID,
                    \"'9e90t3nOfBJS3oQuFn7MzI3v0G1s'\" AS \"'9e90t3nOfBJS3oQuFn7MzI3v0G1s'\",
                    \"'Wzlyy2fuuDSUiTmzhqIq4dGVV1QB'\" AS \"'Wzlyy2fuuDSUiTmzhqIq4dGVV1QB'\"
                FROM (
                    SELECT ID, BEACON_ID, DIFF_FRAC_FLOOR
                    FROM test_data
                )
                PIVOT(SUM(DIFF_FRAC_FLOOR) FOR BEACON_ID IN ('9e90t3nOfBJS3oQuFn7MzI3v0G1s','Wzlyy2fuuDSUiTmzhqIq4dGVV1QB'))
            )
        ";
        
        // Process the SQL through the grammar
        $processedSql = $grammar->processColumns($sql);
        
        // Verify the SQL was processed without errors
        $this->assertNotNull($processedSql);
        
        // Check that the column names with quotes are handled correctly
        $this->assertStringContainsString(
            "ARRAY_AGG(\"'9e90t3nOfBJS3oQuFn7MzI3v0G1s'\")",
            $processedSql
        );
        
        // If the test fails, output the processed SQL for debugging
        if (!str_contains($processedSql, "ARRAY_AGG(\"'9e90t3nOfBJS3oQuFn7MzI3v0G1s'\")")) {
            echo "Processed SQL: " . $processedSql;
        }
    }
} 