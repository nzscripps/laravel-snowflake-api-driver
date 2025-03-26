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
        
        // Pre-process field mapping once outside the loop
        $fieldMap = [];
        foreach ($this->fields as $index => $field) {
            $fieldMap[$index] = $field['name'] ?? "column_$index";
        }
        
        // Transform data to associative arrays with column names as keys
        $result = array_map(function($row) use ($fieldMap) {
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
                
                // Convert types to native PHP types - only pass type info if needed
                $type = $this->fields[$index]['type'] ?? null;
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

        // Handle date/time types
        if (is_string($value)) {
            try {
                $this->debugLog('Result: Processing date/time value', [
                    'value' => $value,
                    'type' => $type
                ]);

                switch ($type) {
                    case 'DATE':
                        // Handle dates like '2014-12-30'
                        $result = new \DateTime($value . ' 00:00:00');
                        $this->debugLog('Result: Converted DATE value', [
                            'input' => $value,
                            'output' => $result->format('Y-m-d H:i:s'),
                            'timezone' => $result->getTimezone()->getName()
                        ]);
                        return $result;
                        
                    case 'TIME':
                        // Handle times like '00:29:01.000' or '00:30'
                        if (preg_match('/^\d{2}:\d{2}(:\d{2})?(\.\d{3})?$/', $value)) {
                            // Append date to make it a valid DateTime
                            $result = new \DateTime('1970-01-01 ' . $value);
                            $this->debugLog('Result: Converted TIME value', [
                                'input' => $value,
                                'output' => $result->format('H:i:s.u'),
                                'timezone' => $result->getTimezone()->getName()
                            ]);
                            return $result;
                        }
                        $this->debugLog('Result: Invalid TIME format', [
                            'value' => $value
                        ]);
                        break;
                        
                    case 'TIMESTAMP':
                    case 'TIMESTAMP_NTZ': // Timestamp without timezone
                    case 'TIMESTAMP_LTZ': // Timestamp with local timezone
                    case 'TIMESTAMP_TZ':  // Timestamp with timezone
                        // Handle datetime strings like '2024-11-06 00:00:00'
                        if (strpos($value, ' ') !== false) {
                            $result = new \DateTime($value);
                            $this->debugLog('Result: Converted TIMESTAMP value', [
                                'input' => $value,
                                'output' => $result->format('Y-m-d H:i:s.u'),
                                'timezone' => $result->getTimezone()->getName(),
                                'timestamp_type' => $type
                            ]);
                            return $result;
                        }
                        $this->debugLog('Result: Invalid TIMESTAMP format', [
                            'value' => $value,
                            'type' => $type
                        ]);
                        break;
                }
            } catch (\Exception $e) {
                // Log the error but return original value if date parsing fails
                Log::warning('Failed to parse date/time value', [
                    'value' => $value,
                    'type' => $type,
                    'error' => $e->getMessage(),
                    'trace' => $e->getTraceAsString()
                ]);
                $this->debugLog('Result: Date/time parsing failed', [
                    'value' => $value,
                    'type' => $type,
                    'error' => $e->getMessage()
                ]);
                return $value;
            }
        }
        
        // Handle numeric values
        if (is_string($value) && is_numeric($value)) {
            $this->debugLog('Result: Processing numeric value', [
                'value' => $value,
                'type' => $type
            ]);

            // Integer
            if ($type === 'INTEGER' || $type === 'BIGINT' || $type === 'SMALLINT' || $type === 'TINYINT') {
                $result = (int)$value;
                $this->debugLog('Result: Converted to integer', [
                    'input' => $value,
                    'output' => $result,
                    'type' => $type
                ]);
                return $result;
            }
            
            // Float/double/decimal - use float for PHP representation
            if ($type === 'FLOAT' || $type === 'DOUBLE' || $type === 'DECIMAL' || $type === 'NUMERIC' || $type === 'REAL') {
                $result = (float)$value;
                $this->debugLog('Result: Converted to float', [
                    'input' => $value,
                    'output' => $result,
                    'type' => $type
                ]);
                return $result;
            }
            
            // For other numeric-looking strings, convert to appropriate type
            if (filter_var($value, FILTER_VALIDATE_INT) !== false) {
                $result = (int)$value;
                $this->debugLog('Result: Auto-converted to integer', [
                    'input' => $value,
                    'output' => $result
                ]);
                return $result;
            }
            
            if (filter_var($value, FILTER_VALIDATE_FLOAT) !== false) {
                $result = (float)$value;
                $this->debugLog('Result: Auto-converted to float', [
                    'input' => $value,
                    'output' => $result
                ]);
                return $result;
            }
        }
        
        // Return original value for other types
        $this->debugLog('Result: Returning original value', [
            'value' => $value,
            'type' => $type
        ]);
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
