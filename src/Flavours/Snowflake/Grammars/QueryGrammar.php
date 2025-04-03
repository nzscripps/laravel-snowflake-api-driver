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
        
        // Extract column names from the first set of values
        if (empty($values)) {
            return "insert into {$this->wrapTable($query->from)} default values";
        }
        
        $firstValue = reset($values);
        $columnsProvided = false;
        $allNumeric = false;
        
        // Check if columns are provided as property in the query builder
        if (isset($query->columns) && !empty($query->columns)) {
            $columns = $query->columns;
            $columnsProvided = true;
            $this->debugLog('Using columns from query builder', ['columns' => $columns, 'file' => __FILE__, 'line' => __LINE__]);
        } 
        // Check if array keys are numeric (positional) or named
        else if (is_array($firstValue)) {
            $keys = array_keys($firstValue);
            $allNumeric = true;
            
            foreach ($keys as $key) {
                if (!is_numeric($key)) {
                    $allNumeric = false;
                    break;
                }
            }
            
            if ($allNumeric) {
                $this->debugLog('Detected numeric keys in first row', ['keys' => $keys, 'file' => __FILE__, 'line' => __LINE__]);
                // For numeric keys, we need columns from elsewhere
                if (isset($query->columns) && !empty($query->columns)) {
                    $columns = $query->columns;
                    $this->debugLog('Using columns from query builder for numeric keys', ['columns' => $columns, 'file' => __FILE__, 'line' => __LINE__]);
                } else {
                    // If we don't have column names at all, generate placeholders (col_0, col_1, etc.)
                    $columns = [];
                    foreach ($keys as $i) {
                        $columns[] = "col_" . $i;
                    }
                    $this->debugLog('Generated placeholder column names for numeric keys', ['columns' => $columns, 'file' => __FILE__, 'line' => __LINE__]);
                }
            } else {
                // For named keys, use the keys as column names
                $columns = $keys;
                $this->debugLog('Using named keys as column names', ['columns' => $columns, 'file' => __FILE__, 'line' => __LINE__]);
            }
        } else {
            // If it's not an array, just use a generic column name
            $columns = ['value'];
            $this->debugLog('Using generic column name for non-array value', ['columns' => $columns, 'file' => __FILE__, 'line' => __LINE__]);
        }
        
        // Format the columns for SQL
        $formattedColumns = $this->columnize($columns);
        
        // Begin the SQL statement
        $sql = "insert into {$this->wrapTable($query->from)} ({$formattedColumns}) values ";
        
        // Get the values part
        $sqlValues = [];
        foreach ($values as $record) {
            $formattedValues = [];
            if (is_array($record)) {
                if ($columnsProvided || $allNumeric) {
                    // If columns were provided or keys are numeric, use values in order
                    foreach (array_values($record) as $value) {
                        $formattedValues[] = $this->parameter($value);
                    }
                } else {
                    // Otherwise use the associative array mapping
                    foreach ($columns as $column) {
                        $value = $record[$column] ?? null;
                        $formattedValues[] = $this->parameter($value);
                    }
                }
            } else {
                // If it's a scalar value, just use it directly
                $formattedValues[] = $this->parameter($record);
            }
            
            $sqlValues[] = '(' . implode(', ', $formattedValues) . ')';
        }
        
        $finalSql = $sql . implode(', ', $sqlValues);
        $this->debugLog('Final SQL insert statement', ['sql' => $finalSql, 'file' => __FILE__, 'line' => __LINE__]);
        return $finalSql;
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
