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
        
        $result = [];
        
        // Transform data to use column names as keys
        foreach ($this->data as $row) {
            $associativeRow = [];
            foreach ($this->fields as $index => $field) {
                $columnName = $field['name'] ?? "column_$index";
                $associativeRow[$columnName] = $row[$index] ?? null;
            }
            $result[] = $associativeRow;
        }
        
        $this->debugLog('Result: Transformed data to associative array', [
            'result_count' => count($result)
        ]);
        
        return $result;
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
