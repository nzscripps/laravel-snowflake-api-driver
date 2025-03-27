<?php

namespace Tests;

use Illuminate\Support\Facades\Log;
use Exception;

trait TestDataManager
{
    /**
     * Get a unique test table name to avoid conflicts
     * 
     * @return string
     */
    protected function getTestTableName()
    {
        return 'SNOWFLAKE_API_TEST_TABLE';
    }
    
    /**
     * Set up test data in Snowflake
     *
     * @return void
     */
    protected function setupTestData()
    {
        if (!$this->service) {
            return;
        }
        
        try {
            // Get a unique table name to avoid conflicts
            $testTable = $this->getTestTableName();
            
            // Verify current schema
            $schemaResult = $this->service->ExecuteQuery('SELECT CURRENT_SCHEMA()');
            Log::debug('TestDataManager: Current schema', $schemaResult->toArray());
            
            // Drop table if it already exists to ensure clean state
            Log::debug('TestDataManager: Dropping test table if it exists');
            $this->service->ExecuteQuery("DROP TABLE IF EXISTS {$testTable}");
            
            // Execute statements one at a time
            Log::debug('TestDataManager: Creating test table');
            $createTable = "CREATE OR REPLACE TABLE {$testTable} (
                id INTEGER,
                string_col VARCHAR,
                date_col DATE,
                bool_col BOOLEAN
            )";
            $this->service->ExecuteQuery($createTable);
            
            Log::debug('TestDataManager: Inserting test data');
            $insertData = "INSERT INTO {$testTable} VALUES 
                (1, 'test1', '2023-01-01', true),
                (2, 'test2', '2023-01-02', false)";
            $this->service->ExecuteQuery($insertData);
            
            // Verify table exists
            $tableResult = $this->service->ExecuteQuery("SHOW TABLES LIKE '{$testTable}'");
            Log::debug('TestDataManager: Table created', $tableResult->toArray());
            
            Log::info('TestDataManager: Successfully set up test data');
        } catch (Exception $e) {
            Log::warning('TestDataManager: Failed to set up test data', [
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString()
            ]);
            
            // Continue test execution even if setup fails
        }
    }
    
    /**
     * Clean up test data from Snowflake
     *
     * @return void
     */
    protected function cleanupTestData()
    {
        if (!$this->service) {
            return;
        }
        
        try {
            // Get the same unique table name
            $testTable = $this->getTestTableName();
            
            // Drop test table 
            Log::debug("TestDataManager: Dropping test table {$testTable}");
            $this->service->ExecuteQuery("DROP TABLE IF EXISTS {$testTable}");
            Log::info('TestDataManager: Successfully cleaned up test table');
        } catch (Exception $e) {
            Log::warning('TestDataManager: Failed to clean up test data', [
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString()
            ]);
            
            // Continue test execution even if cleanup fails
        }
    }
    
    /**
     * Create a mock response data array with common structure
     *
     * @param array $data The actual row data
     * @return array The mock response structure
     */
    protected function createMockResponseData(array $data)
    {
        $rowType = [];
        
        // Infer types from the first row if available
        if (!empty($data) && isset($data[0]) && is_array($data[0])) {
            foreach ($data[0] as $key => $value) {
                $type = $this->inferType($value);
                $rowType[] = [
                    'name' => $key,
                    'type' => $type
                ];
            }
        }
        
        return [
            'code' => '090001', // Success code
            'data' => $data,
            'resultSetMetaData' => [
                'numRows' => count($data),
                'partitionInfo' => [0], // Single partition
                'rowType' => $rowType
            ],
            'createdOn' => time()
        ];
    }
    
    /**
     * Infer a Snowflake data type from a PHP value
     *
     * @param mixed $value The value to infer the type from
     * @return string The Snowflake data type
     */
    private function inferType($value)
    {
        if (is_bool($value)) {
            return 'BOOLEAN';
        }
        
        if (is_int($value)) {
            return 'INTEGER';
        }
        
        if (is_float($value)) {
            return 'FLOAT';
        }
        
        if ($value instanceof \DateTime) {
            return 'TIMESTAMP_NTZ';
        }
        
        return 'VARCHAR';
    }
} 