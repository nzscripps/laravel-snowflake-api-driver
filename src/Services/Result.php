<?php

namespace LaravelSnowflakeApi\Services;

use Illuminate\Support\Collection;
use Illuminate\Support\Facades\Log;
use Exception;

class Result
{
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
        Log::info('Result: Initialized with service', [
            'has_service' => !is_null($service)
        ]);
        $this->service = $service;
    }

    public function setId($id)
    {
        Log::info('Result: Setting ID', ['id' => $id]);
        $this->id = $id;
    }

    public function getId()
    {
        return $this->id;
    }

    public function setExecuted($executed)
    {
        Log::info('Result: Setting execution status', ['executed' => $executed]);
        $this->executed = $executed;
    }

    public function isExecuted()
    {
        return $this->executed;
    }

    public function setTotal($total)
    {
        Log::info('Result: Setting total row count', ['total' => $total]);
        $this->total = $total;
    }

    public function getTotal()
    {
        return $this->total;
    }

    public function setPage($page)
    {
        Log::info('Result: Setting current page', ['page' => $page]);
        $this->page = $page;
    }

    public function getPage()
    {
        return $this->page;
    }

    public function setPageTotal($pageTotal)
    {
        Log::info('Result: Setting total pages', ['page_total' => $pageTotal]);
        $this->pageTotal = $pageTotal;
    }

    public function getPageTotal()
    {
        return $this->pageTotal;
    }

    public function setFields($fields)
    {
        Log::info('Result: Setting fields metadata', [
            'field_count' => count($fields)
        ]);
        $this->fields = $fields;
    }

    public function getFields()
    {
        return $this->fields;
    }

    public function setData($data)
    {
        Log::info('Result: Setting data', [
            'data_count' => count($data)
        ]);
        $this->data = $data;
    }

    public function getData()
    {
        return $this->data;
    }

    public function setTimestamp($timestamp)
    {
        Log::info('Result: Setting timestamp', ['timestamp' => $timestamp]);
        $this->timestamp = $timestamp;
    }

    public function getTimestamp()
    {
        return $this->timestamp;
    }

    public function getPaginationNext()
    {
        Log::info('Result: Checking for next page', [
            'current_page' => $this->page,
            'total_pages' => $this->pageTotal
        ]);

        if ($this->page < $this->pageTotal) {
            try {
                $this->page++;
                Log::info('Result: Fetching next page', ['next_page' => $this->page]);

                $data = $this->service->getStatement($this->id, $this->page);
                Log::info('Result: Next page data retrieved', [
                    'data_count' => isset($data['data']) ? count($data['data']) : 0
                ]);

                $this->setData($data['data']);
                return true;
            } catch (Exception $e) {
                Log::error('Result: Error getting next page', [
                    'page' => $this->page,
                    'error' => $e->getMessage(),
                    'trace' => $e->getTraceAsString()
                ]);
                throw $e;
            }
        }

        Log::info('Result: No more pages available');
        return false;
    }

    /**
     * Convert the result to an array
     *
     * @return array
     */
    public function toArray()
    {
        Log::info('Result: Converting to array', [
            'data_count' => count($this->data)
        ]);
        return $this->data;
    }

    /**
     * Get the number of items in the result
     *
     * @return int
     */
    public function count()
    {
        $count = count($this->data);
        Log::info('Result: Counting data', ['count' => $count]);
        return $count;
    }

    /**
     * Convert the result to a Laravel Collection
     *
     * @return \Illuminate\Support\Collection
     */
    public function toCollection()
    {
        Log::info('Result: Converting to collection', [
            'data_count' => count($this->data)
        ]);
        return new Collection($this->data);
    }
}
