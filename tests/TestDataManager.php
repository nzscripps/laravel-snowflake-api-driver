<?php

namespace Tests;

trait TestDataManager
{
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
        
        $sql = file_get_contents(__DIR__ . '/fixtures/setup.sql');
        $this->service->ExecuteQuery($sql);
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
        
        $this->service->ExecuteQuery('DROP SCHEMA IF EXISTS test_schema CASCADE');
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