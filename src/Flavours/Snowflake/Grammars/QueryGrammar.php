<?php
namespace LaravelSnowflakeApi\Flavours\Snowflake\Grammars;

use Illuminate\Support\Str;
use Illuminate\Support\Facades\Log;
use Illuminate\Database\Query\Builder;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Database\Query\Grammars\Grammar;
use Illuminate\Database\Schema\ColumnDefinition;

class QueryGrammar extends Grammar
{
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
        return parent::compileSelect($query);
    }

    /**
     * Convert an array of column names into a delimited string.
     *
     * @return string
     */
    public function columnize(array $columns)
    {
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
        if (method_exists($this, 'isExpression') && !$this->isExpression($table)) {
            $table = $this->preWrapTable($table);
            return $this->wrap($this->tablePrefix . $table);
        }

        return $this->getValue($table);
    }

    public static function preWrapTable($tableName)
    {
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
        return $expression instanceof Expression ? $expression->getValue($this) : $expression;
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
        return collect($segments)->map(function ($segment, $key) use ($segments) {
            return 0 === $key && count($segments) > 1
                ? $this->wrapTable($segment)
                // Original ->wraValue, but this is always called for columns segments
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
        return 'limit ' . $limit;
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
     * Compile the "offset" portions of the query.
     *
     * @param  \Illuminate\Database\Query\Builder  $query
     * @param  int  $offset
     * @return string
     */
    protected function compileOffset(Builder $query, $offset)
    {
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
        // Snowflake doesn't support table locking
        return '';
    }

    /**
     * Wrap a table in keyword identifiers.
     *
     * @param  string  $table
     * @return string
     */
    public function wrapTable($table)
    {
        if (method_exists($this, 'isExpression') && $this->isExpression($table)) {
            $table = $this->getValue($table);
        }

        Log::info('wrapTable', ['table' => $table, 'file' => __FILE__, 'line' => __LINE__]);
        if (! env('SNOWFLAKE_COLUMNS_CASE_SENSITIVE', false)) {
            $table = Str::upper($table);
        }

        return parent::wrapTable($table);
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
        return DB::connection()->getPdo()->quote($value);
    }
}
