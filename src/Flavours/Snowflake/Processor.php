<?php

namespace LaravelSnowflakeApi\Flavours\Snowflake;

use Illuminate\Support\Str;
use Illuminate\Support\Facades\Log;
use Illuminate\Database\Query\Builder;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Database\Query\Processors\Processor as BaseProcessor;
use LaravelSnowflakeApi\Traits\DebugLogging;

class Processor extends BaseProcessor
{
    use DebugLogging;

    public static function preWrapTable($tableName)
    {
        if (! env('SNOWFLAKE_COLUMNS_CASE_SENSITIVE', false)) {
            $tableName = Str::upper($tableName);
        }

        return $tableName;
    }

    /**
     * Process the results of a column listing query.
     *
     * @param array $results
     *
     * @return array
     */
    public function processColumnListing($results)
    {
        return array_map(function ($result) {
            return ((object) $result)->column_name;
        }, $results);
    }

    /**
     * Process an "insert get ID" query.
     *
     * @param string      $sql
     * @param array       $values
     * @param string|null $sequence
     *
     * @return int
     */
    public function processInsertGetId(Builder $query, $sql, $values, $sequence = null)
    {
        $connection = $query->getConnection();

        $connection->insert($sql, $values);

        $idColumn = $sequence ?: 'id';
        $wrappedTable = $query->getGrammar()->wrapTable($query->from);

        $result = $connection->selectOne(sprintf('select max("%s") as "%s" from %s', $idColumn, $idColumn, $wrappedTable));

        $id = $result->$idColumn;

        return is_numeric($id) ? (int) $id : $id;
    }

    /**
     * Process the results of a "select" query.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @param  array  $results
     * @return array
     */
    public function processSelect(Builder $query, $results)
    {
        $this->debugLog('processSelect', ['result_count' => count($results)]);
        
        // Results are already processed in the Result class
        return $results;
    }
}
