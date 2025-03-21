<?php

namespace LaravelSnowflakeApi\Services;

use Illuminate\Support\Collection;
use Illuminate\Support\Facades\Log;
use Exception;
use LaravelSnowflakeApi\Traits\DebugLogging;

class Result
{
    use DebugLogging;

    private $id;
    private $executed = false;
    private $total = 0;
    private $page = 0;
    private $pageTotal = 0;
    private $fields = [];
    private $data = [];
    private $timestamp;
    private $service;

    public function __construct($service = null)
    {
        $this->debugLog('Result: Initialized with service', [
            'has_service' => !is_null($service)
        ]);
        $this->service = $service;
    }

    public function setId($id)
    {
        $this->debugLog('Result: Setting ID', ['id' => $id]);
        $this->id = $id;
        return $this;
    }

    public function getId()
    {
        return $this->id;
    }

    public function setExecuted($executed)
    {
        $this->debugLog('Result: Setting execution status', ['executed' => $executed]);
        $this->executed = $executed;
        return $this;
    }

    public function isExecuted()
    {
        return $this->executed;
    }

    public function setTotal($total)
    {
        $this->debugLog('Result: Setting total row count', ['total' => $total]);
        $this->total = $total;
        return $this;
    }

    public function getTotal()
    {
        return $this->total;
    }

    public function setPage($page)
    {
        $this->debugLog('Result: Setting current page', ['page' => $page]);
        $this->page = $page;
        return $this;
    }

    public function getPage()
    {
        return $this->page;
    }

    public function setPageTotal($pageTotal)
    {
        $this->debugLog('Result: Setting total pages', ['page_total' => $pageTotal]);
        $this->pageTotal = $pageTotal;
        return $this;
    }

    public function getPageTotal()
    {
        return $this->pageTotal;
    }

    public function setFields($fields)
    {
        $this->debugLog('Result: Setting fields metadata', [
            'field_count' => count($fields)
        ]);
        $this->fields = $fields;
        return $this;
    }

    public function getFields()
    {
        return $this->fields;
    }

    public function setData($data)
    {
        $this->debugLog('Result: Setting data', [
            'data_count' => count($data)
        ]);
        $this->data = $data;
        return $this;
    }

    public function getData()
    {
        return $this->data;
    }

    /**
     * Add data from additional result pages
     *
     * @param array $pageData Data from an additional page
     * @return $this
     */
    public function addPageData(array $pageData)
    {
        $this->debugLog('Result: Adding page data', [
            'existing_data_count' => count($this->data),
            'new_data_count' => count($pageData)
        ]);
        
        // Append the new data to the existing data array
        $this->data = array_merge($this->data, $pageData);
        
        $this->debugLog('Result: Data merged successfully', [
            'total_data_count' => count($this->data)
        ]);
        
        return $this;
    }

    public function setTimestamp($timestamp)
    {
        $this->debugLog('Result: Setting timestamp', ['timestamp' => $timestamp]);
        $this->timestamp = $timestamp;
        return $this;
    }

    public function getTimestamp()
    {
        return $this->timestamp;
    }

    public function getPaginationNext()
    {
        $this->debugLog('Result: Checking for next page', [
            'current_page' => $this->page,
            'total_pages' => $this->pageTotal
        ]);

        if ($this->page < $this->pageTotal) {
            try {
                $this->page++;
                $this->debugLog('Result: Fetching next page', ['next_page' => $this->page]);

                $data = $this->service->getStatement($this->id, $this->page);
                $this->debugLog('Result: Next page data retrieved', [
                    'data_count' => isset($data['data']) ? count($data['data']) : 0
                ]);

                $this->setData($data['data']);
                return true;
            } catch (Exception $e) {
                $this->debugLog('Result: Error getting next page', [
                    'page' => $this->page,
                    'error' => $e->getMessage(),
                    'trace' => $e->getTraceAsString()
                ]);
                throw $e;
            }
        }

        $this->debugLog('Result: No more pages available');
        return false;
    }

    /**
     * Convert the result to an array
     *
     * @return array
     */
    public function toArray()
    {
        $this->debugLog('Result: Converting to array', [
            'data_count' => count($this->data),
            'fields_count' => count($this->fields)
        ]);
        
        // Pre-process field mapping and type info once outside the loop
        $fieldMap = [];
        $typeMap = [];
        foreach ($this->fields as $index => $field) {
            $fieldMap[$index] = $field['name'] ?? "column_$index";
            $typeMap[$index] = $field['type'] ?? null;
        }
        
        // Transform data to associative arrays with column names as keys
        $result = array_map(function($row) use ($fieldMap, $typeMap) {
            $rowData = [];
            
            // Process only the needed indices based on field map
            foreach ($fieldMap as $index => $columnName) {
                if (!isset($row[$index])) {
                    $rowData[$columnName] = null;
                    continue;
                }
                
                $value = $row[$index];
                
                // Handle case where value is wrapped in an "Item" object
                if (is_array($value) && count($value) === 1 && isset($value['Item'])) {
                    $value = $value['Item'];
                }
                
                // Convert types to native PHP types only when necessary
                $type = $typeMap[$index];
                
                // Skip type conversion for common types that don't need conversion
                if ($value === null || 
                    (is_numeric($value) && !is_string($value)) || 
                    (is_string($value) && empty($type))) {
                    $rowData[$columnName] = $value;
                } else {
                    $rowData[$columnName] = $this->convertToNativeType($value, $type);
                }
            }
            
            return $rowData;
        }, $this->data);
        
        $this->debugLog('Result: Transformed data to array format', [
            'result_count' => count($result)
        ]);
        
        return $result;
    }

    /**
     * Convert a value to its native PHP type based on Snowflake type
     *
     * @param mixed $value The value to convert
     * @param string|null $type The Snowflake data type
     * @return mixed The converted value
     */
    protected function convertToNativeType($value, $type = null)
    {
        // Handle null values
        if ($value === null) {
            return null;
        }
        
        // Handle boolean values
        if (is_string($value) && ($type === 'BOOLEAN' || strtolower($value) === 'true' || strtolower($value) === 'false')) {
            return filter_var($value, FILTER_VALIDATE_BOOLEAN);
        }
        
        // Handle numeric values
        if (is_string($value) && is_numeric($value)) {
            // Integer
            if ($type === 'INTEGER' || $type === 'BIGINT' || $type === 'SMALLINT' || $type === 'TINYINT') {
                return (int)$value;
            }
            
            // Float/double/decimal - use float for PHP representation
            if ($type === 'FLOAT' || $type === 'DOUBLE' || $type === 'DECIMAL' || $type === 'NUMERIC' || $type === 'REAL') {
                return (float)$value;
            }
            
            // For other numeric-looking strings, convert to appropriate type
            if (filter_var($value, FILTER_VALIDATE_INT) !== false) {
                return (int)$value;
            }
            
            if (filter_var($value, FILTER_VALIDATE_FLOAT) !== false) {
                return (float)$value;
            }
        }
        
        // Return original value for other types
        return $value;
    }

    /**
     * Get the number of items in the result
     *
     * @return int
     */
    public function count()
    {
        $count = count($this->data);
        $this->debugLog('Result: Counting data', ['count' => $count]);
        return $count;
    }

    /**
     * Convert the result to a Laravel Collection
     *
     * @return \Illuminate\Support\Collection
     */
    public function toCollection()
    {
        $this->debugLog('Result: Converting to collection');
        return new Collection($this->toArray());
    }
}
