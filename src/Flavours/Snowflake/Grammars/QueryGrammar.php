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

    public static function preWrapTable($tableName)
    {
        self::debugLog('preWrapTable', ['tableName' => $tableName, 'file' => __FILE__, 'line' => __LINE__]);
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
}
