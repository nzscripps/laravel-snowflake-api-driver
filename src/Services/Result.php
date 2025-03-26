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
        
        // Early return for empty datasets
        if (empty($this->data)) {
            return [];
        }
        
        // Pre-process field mapping once outside the loop
        $fieldMap = [];
        $fieldTypes = [];
        
        foreach ($this->fields as $index => $field) {
            $fieldMap[$index] = $field['name'] ?? "column_$index";
            $fieldTypes[$index] = $field['type'] ?? null;
        }
        
        // Transform data to associative arrays with column names as keys
        $result = array_map(function($row) use ($fieldMap, $fieldTypes) {
            $rowData = [];
            
            // Process each field once
            foreach ($fieldMap as $index => $columnName) {
                // Handle missing values
                if (!isset($row[$index])) {
                    $rowData[$columnName] = null;
                    continue;
                }
                
                $value = $row[$index];
                
                // Unwrap Item objects
                if (is_array($value) && count($value) === 1 && isset($value['Item'])) {
                    $value = $value['Item'];
                }
                
                // Get the type and convert value
                $type = $fieldTypes[$index];
                $rowData[$columnName] = $this->convertToNativeType($value, $type);
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
        $this->debugLog('Result: Starting type conversion', [
            'value' => $value,
            'type' => $type,
            'value_type' => gettype($value)
        ]);

        // Handle null values
        if ($value === null) {
            $this->debugLog('Result: Converting null value');
            return null;
        }
        
        // Handle boolean values
        if (is_string($value) && ($type === 'BOOLEAN' || strtolower($value) === 'true' || strtolower($value) === 'false')) {
            $result = filter_var($value, FILTER_VALIDATE_BOOLEAN);
            $this->debugLog('Result: Converting boolean value', [
                'input' => $value,
                'output' => $result
            ]);
            return $result;
        }

        // Handle date/time string formats (now that Snowflake returns formatted strings)
        if (is_string($value) && in_array($type, ['DATE', 'TIME', 'TIMESTAMP', 'TIMESTAMP_NTZ', 'TIMESTAMP_LTZ', 'TIMESTAMP_TZ'])) {
            try {
                $this->debugLog('Result: Processing date/time value', [
                    'value' => $value,
                    'type' => $type
                ]);

                // Use a consistent approach for all date/time types
                switch ($type) {
                    case 'DATE':
                        // Format: YYYY-MM-DD
                        $result = new \DateTime($value . ' 00:00:00');
                        $this->debugLog('Result: Converted DATE value', [
                            'input' => $value,
                            'output' => $result->format('Y-m-d')
                        ]);
                        return $result;
                        
                    case 'TIME':
                        // Format: HH:MI:SS.FF
                        $result = new \DateTime('1970-01-01 ' . $value);
                        $this->debugLog('Result: Converted TIME value', [
                            'input' => $value,
                            'output' => $result->format('H:i:s.u')
                        ]);
                        return $result;
                        
                    case 'TIMESTAMP':
                    case 'TIMESTAMP_NTZ':
                    case 'TIMESTAMP_LTZ':
                    case 'TIMESTAMP_TZ':
                        // Format: YYYY-MM-DD HH:MI:SS.FF [TZH:TZM]
                        $result = new \DateTime($value);
                        $this->debugLog('Result: Converted TIMESTAMP value', [
                            'input' => $value,
                            'output' => $result->format('Y-m-d H:i:s.u'),
                            'timezone' => $result->getTimezone()->getName()
                        ]);
                        return $result;
                }
            } catch (\Exception $e) {
                // Log the error but return original value if date parsing fails
                Log::warning('Failed to parse date/time value', [
                    'value' => $value,
                    'type' => $type,
                    'error' => $e->getMessage()
                ]);
                $this->debugLog('Result: Date/time parsing failed', [
                    'value' => $value,
                    'type' => $type,
                    'error' => $e->getMessage()
                ]);
                return $value;
            }
        }

        // Fallback handling for numeric date values (shouldn't happen with new API parameter settings, but kept for safety)
        if (($type === 'DATE' || strpos($type, 'TIMESTAMP') === 0) && 
            (is_numeric($value) || (is_string($value) && is_numeric($value)))) {
            
            try {
                $numericValue = is_numeric($value) ? $value : (int)$value;
                $this->debugLog('Result: Processing numeric date/timestamp value', [
                    'value' => $value,
                    'numeric_value' => $numericValue,
                    'type' => $type
                ]);
                
                if ($type === 'DATE') {
                    // Convert epoch days to DateTime
                    $dateTime = new \DateTime('1970-01-01');
                    $dateTime->modify("+$numericValue days");
                    return $dateTime;
                }
                
                if (strpos($type, 'TIMESTAMP') === 0) {
                    // Convert epoch microseconds to DateTime
                    $seconds = floor($numericValue / 1000000);
                    $microseconds = $numericValue % 1000000;
                    return \DateTime::createFromFormat('U.u', sprintf('%d.%06d', $seconds, $microseconds));
                }
            } catch (\Exception $e) {
                $this->debugLog('Result: Numeric date/time parsing failed', [
                    'value' => $value,
                    'type' => $type,
                    'error' => $e->getMessage()
                ]);
            }
        }
        
        // Handle numeric values
        if (is_string($value) && is_numeric($value)) {
            // Integer types
            if (in_array($type, ['INTEGER', 'BIGINT', 'SMALLINT', 'TINYINT'])) {
                return (int)$value;
            }
            
            // Float types
            if (in_array($type, ['FLOAT', 'DOUBLE', 'DECIMAL', 'NUMERIC', 'REAL'])) {
                return (float)$value;
            }
            
            // Auto-detect numeric types
            if (filter_var($value, FILTER_VALIDATE_INT) !== false) {
                return (int)$value;
            }
            
            if (filter_var($value, FILTER_VALIDATE_FLOAT) !== false) {
                return (float)$value;
            }
        }
        
        // Return original value for all other types
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
