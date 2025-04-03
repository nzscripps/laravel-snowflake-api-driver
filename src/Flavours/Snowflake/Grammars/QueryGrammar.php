<?php
namespace LaravelSnowflakeApi\Flavours\Snowflake\Grammars;

use Illuminate\Support\Str;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\DB;
use Illuminate\Database\Query\Builder;
use Illuminate\Database\Query\Expression;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Database\Query\Grammars\Grammar;
use Illuminate\Database\Schema\ColumnDefinition;
use LaravelSnowflakeApi\Traits\DebugLogging;

class QueryGrammar extends Grammar
{
    use DebugLogging;

    /**
     * The components that make up a select clause.
     *
     * @var string[]
     */
    protected $selectComponents = [
        'aggregate',
        'columns',
        'from',
        'joins',
        'wheres',
        'groups',
        'havings',
        'orders',
        'limit',
        'offset',
        'unions',
        'lock',
    ];

    /**
     * Compile a select query into SQL.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @return string
     */
    public function compileSelect(Builder $query)
    {
        $this->debugLog('Compile Select', ['file' => __FILE__, 'line' => __LINE__]);
        return parent::compileSelect($query);
    }

    /**
     * Convert an array of column names into a delimited string.
     *
     * @return string
     */
    public function columnize(array $columns)
    {
        $this->debugLog('columnize', ['columns' => $columns, 'file' => __FILE__, 'line' => __LINE__]);
        return implode(', ', array_map([$this, 'wrapColumn'], $columns));
    }

    /**
     * Wrap a table in keyword identifiers.
     *
     * @param \Illuminate\Database\Query\Expression|string $table
     *
     * @return string
     */
    public function wrapTable($table)
    {
        $this->debugLog('wrapTable', ['table' => $table, 'file' => __FILE__, 'line' => __LINE__]);
        if (method_exists($this, 'isExpression') && !$this->isExpression($table)) {
            $table = $this->preWrapTable($table);
            return $this->wrap($this->tablePrefix . $table);
        }

        return $this->getValue($table);
    }

    protected function preWrapTable($tableName)
    {
        $this->debugLog('preWrapTable', ['tableName' => $tableName, 'file' => __FILE__, 'line' => __LINE__]);
        if ($tableName instanceof Blueprint) {
            $tableName = $tableName->getTable();
        }

        if (! env('SNOWFLAKE_COLUMNS_CASE_SENSITIVE', false)) {
            $tableName = Str::upper($tableName);
        }

        return $tableName;
    }

    /**
     * Get the value of a raw expression.
     *
     * @param \Illuminate\Database\Query\Expression $expression
     *
     * @return string
     */
    public function getValue($expression)
    {
        $this->debugLog('getValue', ['expression' => $expression, 'file' => __FILE__, 'line' => __LINE__]);
        $return = $expression->getValue($this);
        $this->debugLog('display Return', ['return' => $return, 'file' => __FILE__, 'line' => __LINE__]);
        return $return;
    }

    /**
     * Wrap the given value segments.
     *
     * @param array $segments
     *
     * @return string
     */
    protected function wrapSegments($segments)
    {
        $this->debugLog('wrapSegments', ['segments' => $segments, 'file' => __FILE__, 'line' => __LINE__]);
        return collect($segments)->map(function ($segment, $key) use ($segments) {
            return 0 === $key && count($segments) > 1
                ? $this->wrapTable($segment)
                : $this->wrapColumn($segment);
        })->implode('.');
    }

    /**
     * Wrap a single string in keyword identifiers.
     *
     * @param string | \Illuminate\Database\Query\Expression $column
     *
     * @return string
     */
    protected function wrapColumn($column)
    {
        $this->debugLog('wrapColumn', ['column' => $column, 'file' => __FILE__, 'line' => __LINE__]);
        if (method_exists($this, 'isExpression') && $this->isExpression($column)) {
            return $this->getValue($column);
        }

        if ($column instanceof ColumnDefinition) {
            $column = $column->get('name');
        }

        if ('*' !== $column) {
            return str_replace('"', '', $column);
        }

        return $column;
    }

    /**
     * Wrap a single string in keypublic function wrapTable($table)word identifiers.
     *
     * @param string $value
     *
     * @return string
     */
    protected function wrapValue($value)
    {
        $this->debugLog('wrapValue', ['value' => $value, 'file' => __FILE__, 'line' => __LINE__]);
        if ('*' !== $value) {
            return "'".str_replace("'", "''", $value)."'";
        }

        return $value;
    }

    /**
     * Compile the "limit" portions of the query.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @param  int  $limit
     * @return string
     */
    protected function compileLimit(Builder $query, $limit)
    {
        $this->debugLog('compileLimit', ['limit' => $limit, 'file' => __FILE__, 'line' => __LINE__]);
        return 'limit ' . $limit;
    }

    /**
     * Compile the "offset" portions of the query.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @param  int  $offset
     * @return string
     */
    protected function compileOffset(Builder $query, $offset)
    {
        $this->debugLog('compileOffset', ['offset' => $offset, 'file' => __FILE__, 'line' => __LINE__]);
        return 'offset ' . $offset;
    }

    /**
     * Compile the lock into SQL.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @param  bool|string  $value
     * @return string
     */
    protected function compileLock(Builder $query, $value)
    {
        $this->debugLog('compileLock', ['value' => $value, 'file' => __FILE__, 'line' => __LINE__]);
        // Snowflake doesn't support table locking
        return '';
    }

    /**
     * Wrap a union subquery in parentheses.
     *
     * @param string $sql
     *
     * @return string
     */
    protected function wrapUnion($sql)
    {
        $this->debugLog('wrapUnion', ['sql' => $sql, 'file' => __FILE__, 'line' => __LINE__]);
        return 'select * from ('.$sql.')';
    }

    /**
     * Escapes a value for safe SQL embedding.
     *
     * @param  string|float|int|bool|null  $value
     * @param  bool  $binary
     * @return string
     */
    public function escape($value, $binary = false)
    {
        $this->debugLog('escape', ['value' => $value, 'binary' => $binary, 'file' => __FILE__, 'line' => __LINE__]);
        return DB::connection()->getPdo()->quote($value);
    }

    /**
     * Compile an insert statement into SQL.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @param  array  $values
     * @return string
     */
    public function compileInsert(Builder $query, array $values)
    {
        $this->debugLog('compileInsert', ['values_count' => count($values), 'file' => __FILE__, 'line' => __LINE__]);
        
        // If no values, use default values syntax
        if (empty($values)) {
            return "insert into {$this->wrapTable($query->from)} default values";
        }
        
        // Get the first value to analyze structure
        $firstRow = reset($values);
        
        // Determine column names based on available information
        $columns = $this->determineInsertColumns($query, $firstRow);
        
        // Format the columns for SQL
        $formattedColumns = $this->columnize($columns);
        
        // Begin the SQL statement
        $sql = "insert into {$this->wrapTable($query->from)} ({$formattedColumns}) values ";
        
        // Get the values part
        $sqlValues = [];
        foreach ($values as $record) {
            $sqlValues[] = $this->formatInsertValues($record, $columns);
        }
        
        $finalSql = $sql . implode(', ', $sqlValues);
        $this->debugLog('Final SQL insert statement', ['sql' => $finalSql, 'file' => __FILE__, 'line' => __LINE__]);
        return $finalSql;
    }
    
    /**
     * Determine the column names for an insert query
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @param  array|mixed  $firstRow
     * @return array
     */
    protected function determineInsertColumns(Builder $query, $firstRow)
    {
        // Priority 1: Use columns explicitly set on the query builder
        if (isset($query->columns) && !empty($query->columns)) {
            $this->debugLog('Using columns from query builder', ['columns' => $query->columns, 'file' => __FILE__, 'line' => __LINE__]);
            return $query->columns;
        }
        
        // If the first row isn't an array, just use a default column name
        if (!is_array($firstRow)) {
            return ['value'];
        }
        
        // Check if array keys are numeric (positional) or named
        $keys = array_keys($firstRow);
        $allNumeric = $this->hasOnlyNumericKeys($keys);
        
        if ($allNumeric) {
            // For numeric keys with no columns provided, use generic column names
            $this->debugLog('Creating placeholder column names for numeric keys', ['file' => __FILE__, 'line' => __LINE__]);
            return array_map(function($i) {
                return "col_$i";
            }, $keys);
        }
        
        // For associative arrays, use the keys as column names
        $this->debugLog('Using associative keys as column names', ['keys' => $keys, 'file' => __FILE__, 'line' => __LINE__]);
        return $keys;
    }
    
    /**
     * Format values for an insert statement
     *
     * @param  array|mixed  $record
     * @param  array  $columns
     * @return string
     */
    protected function formatInsertValues($record, array $columns)
    {
        // If not an array, just return a single value
        if (!is_array($record)) {
            return '(' . $this->parameter($record) . ')';
        }
        
        $values = [];
        
        // Check if we have numeric keys (positional) or associative array
        $isNumeric = $this->hasOnlyNumericKeys(array_keys($record));
        
        if ($isNumeric) {
            // For numeric keys, use values in order
            foreach (array_values($record) as $value) {
                $values[] = $this->parameter($value);
            }
        } else {
            // For associative arrays, map values to columns
            foreach ($columns as $column) {
                $values[] = $this->parameter($record[$column] ?? null);
            }
        }
        
        return '(' . implode(', ', $values) . ')';
    }
    
    /**
     * Check if an array has only numeric keys
     *
     * @param  array  $keys
     * @return bool
     */
    protected function hasOnlyNumericKeys(array $keys)
    {
        foreach ($keys as $key) {
            if (!is_numeric($key)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Format a value as a parameter for SQL
     *
     * @param  mixed  $value
     * @return string
     */
    public function parameter($value)
    {
        if (is_null($value)) {
            return 'null';
        }
        
        if (is_bool($value)) {
            return $value ? 'true' : 'false';
        }
        
        if (is_numeric($value)) {
            return (string) $value;
        }
        
        if ($value instanceof Expression) {
            return $this->getValue($value);
        }
        
        return "'" . str_replace("'", "''", $value) . "'";
    }

    protected static function debugLog($message, array $context = [])
    {
        if (env('SF_DEBUG', false)) {
            Log::info($message, $context);
        }
    }
}
