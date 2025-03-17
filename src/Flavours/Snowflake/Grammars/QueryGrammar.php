<?php
namespace LaravelSnowflakeApi\Flavours\Snowflake\Grammars;

use Illuminate\Database\Query\Builder;
use Illuminate\Database\Query\Grammars\Grammar;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Str;

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
     * Wrap a single string in keyword identifiers.
     *
     * @param  string  $value
     * @return string
     */
    protected function wrapValue($value)
    {
        if ('*' === $value) {
            return $value;
        }

        // If the value is actually a raw expression, we'll just return it as-is.
        if (strpos($value, 'raw:') === 0) {
            return substr($value, 4);
        }

        if (! env('SNOWFLAKE_COLUMNS_CASE_SENSITIVE', false)) {
            $value = Str::upper($value);
        }

        return '"' . str_replace('"', '""', $value) . '"';
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
}
