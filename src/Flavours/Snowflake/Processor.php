<?php

namespace LaravelSnowflakeApi\Flavours\Snowflake;

use Illuminate\Database\Query\Builder;
use Illuminate\Database\Query\Processors\Processor as BaseProcessor;
use Illuminate\Support\Str;
use LaravelSnowflakeApi\Traits\DebugLogging;

class Processor extends BaseProcessor
{
    use DebugLogging;

    public static function preWrapTable($tableName): string
    {
        if (! env('SNOWFLAKE_COLUMNS_CASE_SENSITIVE', false)) {
            $tableName = Str::upper($tableName);
        }

        return $tableName;
    }

    /**
     * Process the results of a column listing query.
     *
     * @param  array<int, array<string, mixed>>  $results
     * @return array<int, string>
     */
    public function processColumnListing($results): array
    {
        return array_map(function ($result) {
            return ((object) $result)->column_name;
        }, $results);
    }

    /**
     * Process an "insert get ID" query.
     *
     * Note: This implementation performs a second query (select max) which is inefficient
     * and potentially unreliable under concurrency. Snowflake's `LAST_QUERY_ID()`
     * or sequence usage might be better alternatives if the API supports them.
     *
     * @param  string  $sql
     * @param  array  $values
     * @param  string|null  $sequence
     * @return int|string The auto-incrementing ID.
     */
    public function processInsertGetId(Builder $query, $sql, $values, $sequence = null): int|string
    {
        /** @var \LaravelSnowflakeApi\SnowflakeApiConnection $connection */
        $connection = $query->getConnection();

        $connection->insert($sql, $values);

        $idColumn = $sequence ?: 'id';
        $grammar = $query->getGrammar();
        $wrappedTable = $grammar->wrapTable($query->from);
        $wrappedIdColumn = $grammar->wrap($idColumn);

        $result = $connection->selectOne(sprintf('select max(%s) as %s from %s', $wrappedIdColumn, $wrappedIdColumn, $wrappedTable));

        if (! $result) {
            return 0;
        }

        $id = is_object($result) ? $result->{$idColumn} : ($result[$idColumn] ?? null);

        return is_numeric($id) ? (int) $id : (string) $id;
    }

    /**
     * Process the results of a "select" query.
     * The base processor doesn't do much here, so this override might be unnecessary.
     *
     * @param  array  $results
     */
    public function processSelect(Builder $query, $results): array
    {
        $this->debugLog('processSelect', ['result_count' => count($results)]);

        return $results;
    }
}
