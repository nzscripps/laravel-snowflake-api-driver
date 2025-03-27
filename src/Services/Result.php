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
        $this->debugLog('Result: Converting to array');

        if (empty($this->data)) {
            return [];
        }

        $fieldMap = [];
        $fieldTypes = [];
        foreach ($this->fields as $index => $field) {
            $fieldMap[$index] = $field['name'] ?? "column_$index";
            $fieldTypes[$index] = $field['type'] ?? null;
        }

        $result = [];
        foreach ($this->data as $row) {
            $rowData = [];
            foreach ($fieldMap as $index => $columnName) {
                $value = $row[$index] ?? null;

                // Optimized type conversion
                $rowData[$columnName] = $this->convertToNativeType(
                    is_array($value) && isset($value['Item']) ? $value['Item'] : $value,
                    $fieldTypes[$index]
                );
            }
            $result[] = $rowData;
        }

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
        if ($value === null) {
            return null;
        }

        // Single check for wrapped values
        if (is_array($value) && isset($value['Item'])) {
            $value = $value['Item'];
        }

        $type = strtoupper($type);

        // Optimized type handling
        switch(true) {
            case $type === 'BOOLEAN':
                return filter_var($value, FILTER_VALIDATE_BOOLEAN);

            case in_array($type, ['DATE', 'TIME', 'TIMESTAMP']):
                return $this->parseDateTime($value, $type);
                
            case strpos($type, 'TIME_') === 0:
            case strpos($type, 'TIMESTAMP_') === 0:
                return $this->parseDateTime($value, $type);

            case is_numeric($value):
                return strpos($value, '.') !== false ? (float)$value : (int)$value;

            default:
                return $value;
        }
    }

    private function parseDateTime($value, $type)
    {
        try {
            $this->debugLog('Result: Parsing date/time value', [
                'value' => $value,
                'type' => $type,
                'is_string' => is_string($value),
                'string_format' => is_string($value) ? strlen($value) : 'N/A'
            ]);
            
            // Define format mappings for different types
            $formats = [
                'DATE' => 'Y-m-d',
                'TIME' => 'H:i:s',
                'TIME_NTZ' => 'H:i:s',
                'TIMESTAMP' => 'Y-m-d H:i:s',
                'TIMESTAMP_NTZ' => 'Y-m-d H:i:s',
                'TIMESTAMP_LTZ' => 'Y-m-d H:i:s',
                'TIMESTAMP_TZ' => 'Y-m-d H:i:s'
            ];
            
            // Default format based on type
            $format = $formats[$type] ?? 'Y-m-d H:i:s';
            
            // Handle cases when the value is already returned as a string format
            if (is_string($value)) {
                // For TIME values that are already in string format (usually with microseconds)
                if (strpos($type, 'TIME') === 0 && preg_match('/^\d{2}:\d{2}:\d{2}(\.\d+)?$/', $value)) {
                    $timeWithoutMicroseconds = preg_replace('/(\d{2}:\d{2}:\d{2})(\.\d+)?$/', '$1', $value);
                    $this->debugLog('Result: Parsed TIME string value', [
                        'original' => $value,
                        'without_microseconds' => $timeWithoutMicroseconds
                    ]);
                    return $timeWithoutMicroseconds;
                }
                
                // For TIMESTAMP/DATETIME values that are already in string format
                if ((strpos($type, 'TIMESTAMP') === 0 || $type === 'DATETIME') && 
                    preg_match('/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d+)?$/', $value)) {
                    $datetimeWithoutMicroseconds = preg_replace('/(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})(\.\d+)?$/', '$1', $value);
                    $this->debugLog('Result: Parsed TIMESTAMP string value', [
                        'original' => $value,
                        'without_microseconds' => $datetimeWithoutMicroseconds
                    ]);
                    return $datetimeWithoutMicroseconds;
                }
                
                // For DATE values that are already in correct format
                if (strpos($type, 'DATE') === 0 && preg_match('/^\d{4}-\d{2}-\d{2}$/', $value)) {
                    return $value;
                }
            }
            
            // Create a DateTime object to help with formatting
            $this->debugLog('Result: Attempting to create DateTime with format', [
                'format' => $format . '.u',
                'value' => $value
            ]);
            
            // Try to create DateTime with microseconds
            $dateTime = \DateTime::createFromFormat($format . '.u', $value);
            
            // If that fails, try without microseconds
            if (!$dateTime) {
                $this->debugLog('Result: First format failed, trying without microseconds', [
                    'alternate_format' => $format
                ]);
                $dateTime = \DateTime::createFromFormat($format, $value);
            }
            
            // If we successfully created a DateTime object, return formatted string
            if ($dateTime) {
                $formattedString = $dateTime->format($format);
                $this->debugLog('Result: Successfully formatted date/time', [
                    'formatted_string' => $formattedString
                ]);
                return $formattedString;
            }
            
            // If all else fails, return original value
            $this->debugLog('Result: Failed to format date/time, returning original value');
            return $value;
        } catch (\Exception $e) {
            $this->debugLog('Result: Error parsing datetime', [
                'value' => $value,
                'type' => $type,
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString()
            ]);
            return $value;
        }
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
