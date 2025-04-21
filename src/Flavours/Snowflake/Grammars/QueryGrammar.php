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
use Illuminate\Support\Arr;
use Illuminate\Database\Connection;

class QueryGrammar extends Grammar
{
    use DebugLogging;

    /**
     * The database connection instance.
     * Add this property if extending Grammar requires it.
     *
     * @var \Illuminate\Database\Connection
     */
    protected $connection;

    /**
     * The table prefix for queries.
     *
     * @var string
     */
    protected $tablePrefix = '';

    /**
     * Create a new query grammar instance.
     *
     * @param \Illuminate\Database\Connection|null $connection
     * @return void
     */
    public function __construct(?Connection $connection = null)
    {
        // If a connection is provided (normal instantiation), call parent with it; skip otherwise.
        if ($connection) {
            parent::__construct($connection); // Pass the connection to the base Grammar
            $this->setConnection($connection);
        }
        // If $connection is null, the $connection property remains uninitialized
        // until setConnection is called (which should happen in tests or Connection class)
    }

    /**
     * Set the connection instance.
     *
     * @param  \Illuminate\Database\Connection  $connection
     * @return $this
     */
    public function setConnection(Connection $connection)
    {
        $this->connection = $connection;

        return $this;
    }

    /**
     * The components that make up a select clause.
     * Order is important.
     *
     * @var list<string>
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
        'lock',
        // Note: 'unions' are handled separately in compileSelect
    ];

    /**
     * All of the available clause operators.
     * Add Snowflake specific operators if any.
     *
     * @var string[]
     */
    protected $operators = [
        '=', '<>', '!=',
        '<', '<=', '>', '>=',
        'like', 'not like',
        'ilike',
        '&' , '|', '^', // Bitwise operators if supported
        // Add other Snowflake operators like RLIKE, REGEXP, etc. if needed
    ];

    /**
     * Compile a select query into SQL.
     *
     * @param \Illuminate\Database\Query\Builder $query
     * @return string
     */
    public function compileSelect(Builder $query): string
    {
        $this->debugLog('Compile Select', ['file' => __FILE__, 'line' => __LINE__]);
        $sql = parent::compileSelect($query);

        if ($query->unions) {
             $sql = $this->wrapUnion($sql) . ' ' . $this->compileUnions($query);
        }

        $this->debugLog('Compiled Select', ['sql' => $sql, 'file' => __FILE__, 'line' => __LINE__]);
        return $sql;
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
     * @param string|null $prefix
     * @return string
     */
    public function wrapTable($table, $prefix = null): string
    {
        $this->debugLog('wrapTable', ['table' => $table, 'file' => __FILE__, 'line' => __LINE__]);
        if (method_exists($this, 'isExpression') && !$this->isExpression($table)) {
            $tableName = $this->resolveTableName($table);
            $prefixedTableName = ($prefix ?? $this->tablePrefix) . $tableName;
            $this->debugLog('wrapTable', ['resolved_table' => $tableName, 'prefixed_table' => $prefixedTableName, 'file' => __FILE__, 'line' => __LINE__]);
            return $this->wrap($prefixedTableName, true);
        }

        return $this->getValue($table);
    }

    /**
     * Resolve the table name considering potential Blueprint instance and case sensitivity.
     *
     * @param mixed $table
     * @return string
     */
    protected function resolveTableName($table): string
    {
        if ($table instanceof Blueprint) {
            $table = $table->getTable();
        }

        $tableName = (string) $table;

        // Apply case sensitivity setting BEFORE adding prefix
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
     * @param \Illuminate\Database\Query\Builder $query
     * @param int $limit
     * @return string
     */
    protected function compileLimit(Builder $query, $limit): string
    {
        $this->debugLog('compileLimit', ['limit' => $limit, 'file' => __FILE__, 'line' => __LINE__]);
        return 'limit ' . (int) $limit;
    }

    /**
     * Compile the "offset" portions of the query.
     *
     * @param \Illuminate\Database\Query\Builder $query
     * @param int $offset
     * @return string
     */
    protected function compileOffset(Builder $query, $offset): string
    {
        $this->debugLog('compileOffset', ['offset' => $offset, 'file' => __FILE__, 'line' => __LINE__]);
        return 'offset ' . (int) $offset;
    }

    /**
     * Compile the lock into SQL.
     *
     * @param \Illuminate\Database\Query\Builder $query
     * @param bool|string $value
     * @return string
     */
    protected function compileLock(Builder $query, $value): string
    {
        $this->debugLog('compileLock (ignored for Snowflake)', ['value' => $value, 'file' => __FILE__, 'line' => __LINE__]);
        // Snowflake typically doesn't use SELECT ... FOR UPDATE/SHARE.
        // Locking is usually managed implicitly by transaction isolation levels.
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
     * Compile an insert statement into SQL.
     *
     * @param \Illuminate\Database\Query\Builder $query
     * @param array $values
     * @return string
     */
    public function compileInsert(Builder $query, array $values): string
    {
        $this->debugLog('compileInsert', ['values_count' => count($values), 'file' => __FILE__, 'line' => __LINE__]);
        
        $table = $this->wrapTable($query->from);
        
        if (empty($values)) {
            return "insert into {$table} default values";
        }
        
        // Get the first value to analyze structure
        $firstRow = reset($values);
        
        // Determine column names based on available information
        $columns = $this->determineInsertColumns($query, $firstRow);
        
        // Format the columns for SQL
        $formattedColumns = $this->columnize($columns);
        
        // Begin the SQL statement
        $sql = "insert into {$table} ({$formattedColumns}) values ";
        
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
    public function parameter($value): string
    {
        if (is_null($value)) {
            return 'NULL';
        }
        
        if (is_bool($value)) {
            return $value ? 'TRUE' : 'FALSE';
        }
        
        if (is_numeric($value)) {
            return (string) $value;
        }
        
        if ($value instanceof Expression) {
            return $this->getValue($value);
        }
        
        return "'" . str_replace("'", "''", $value) . "'";
    }

    /**
     * Compile an insert statement using columns and values into SQL.
     * Added for compatibility / potential use by Connection::insertWithColumns
     *
     * @param \Illuminate\Database\Query\Builder $query
     * @param array $columns
     * @param array $values Multi-dimensional array of rows
     * @return string
     */
    public function compileInsertWithColumns(Builder $query, array $columns, array $values): string
    {
        $this->debugLog('compileInsertWithColumns', [
            'columns' => $columns,
            'values_count' => count($values),
            'file' => __FILE__, 'line' => __LINE__
        ]);

        $table = $this->wrapTable($query->from);

        if (empty($values)) {
            return "insert into {$table} default values";
        }

        // Ensure values is a multi-dimensional array
        if (! is_array(reset($values))) {
            $values = [$values];
        }

        $formattedColumns = $this->columnize($columns);

        $parameters = collect($values)->map(function ($record) use ($columns) {
            // Ensure record is an array and map values based on $columns order
            $orderedRecord = [];
            if (is_array($record)) {
                foreach ($columns as $column) {
                     $orderedRecord[] = $record[$column] ?? null;
                }
            } else {
                 // Handle non-array record case if necessary, maybe throw error?
                 $orderedRecord = array_fill(0, count($columns), null);
            }
            return '(' . $this->parameter($orderedRecord) . ')';
        })->implode(', ');

        $sql = "insert into {$table} ({$formattedColumns}) values {$parameters}";
        $this->debugLog('Compiled SQL insert statement with columns', ['sql' => $sql, 'file' => __FILE__, 'line' => __LINE__]);
        return $sql;
    }

    /**
     * Compile an insert ignore statement into SQL.
     *
     * @param  \\Illuminate\\Database\\Query\\Builder  $query
     * @param  array  $values
     * @return string
     */
    public function compileInsertOrIgnore(Builder $query, array $values): string
    {
        // Snowflake uses standard INSERT. Duplicate checks would need
        // to be handled via constraints or pre-checks.
        $this->debugLog('compileInsertOrIgnore (using standard INSERT)', ['file' => __FILE__, 'line' => __LINE__]);
        return $this->compileInsert($query, $values);
    }

    /**
     * Compile an "upsert" statement into SQL.
     *
     * @param  \\Illuminate\\Database\\Query\\Builder  $query
     * @param  array  $values
     * @param  array  $uniqueBy
     * @param  array  $update
     * @return string
     */
    public function compileUpsert(Builder $query, array $values, array $uniqueBy, array $update): string
    {
        // Snowflake uses MERGE statement for upserts
        $this->debugLog('compileUpsert (using MERGE)', ['uniqueBy' => $uniqueBy, 'file' => __FILE__, 'line' => __LINE__]);

        $table = $this->wrapTable($query->from);

        if (empty($values)) {
            return ''; // Cannot merge empty values
        }

        // Assume structure of $values allows getting columns from the first row
        $firstRow = Arr::isAssoc(reset($values)) ? reset($values) : $values[0]; // Handle both assoc and numeric arrays
        $columns = $this->columnize(array_keys($firstRow));

        $parameters = collect($values)->map(function ($record) {
            return '(' . $this->parameter($record) . ')';
        })->implode(', ');

        // Create a temporary alias for the source data
        $sourceAlias = 'laravel_upsert_source';
        // Define the source columns matching the structure of $values
        $sourceColumns = $this->columnize(array_map(function ($col) use ($sourceAlias) {
            return "{$sourceAlias}.{$col}";
        }, array_keys($firstRow)));

        // Build the ON condition for the MERGE
        $onCondition = collect($uniqueBy)->map(function ($key) use ($table, $sourceAlias) {
            $wrappedKey = $this->wrap($key);
            return "{$table}.{$wrappedKey} = {$sourceAlias}.{$wrappedKey}";
        })->implode(' AND ');

        // Build the UPDATE part
        $updateAssignments = collect($update)->map(function ($value, $key) use ($table, $sourceAlias) {
            $wrappedKey = $this->wrap($key);
            // If value is not an Expression, use the source value
            $updateValue = $this->isExpression($value) ? $this->getValue($value) : "{$sourceAlias}.{$wrappedKey}";
            return "{$table}.{$wrappedKey} = {$updateValue}";
        })->implode(', ');

        // Build the INSERT part
        $insertColumns = $this->columnize(array_keys($firstRow));
        $insertValues = $this->columnize(array_map(function ($key) use ($sourceAlias) {
            return "{$sourceAlias}.{$this->wrap($key)}";
        }, array_keys($firstRow)));

        $sql = "merge into {$table} using (select {$columns} from values {$parameters} as {$sourceAlias} ({$columns})) as {$sourceAlias} on ({$onCondition})";
        $sql .= " when matched then update set {$updateAssignments}";
        $sql .= " when not matched then insert ({$insertColumns}) values ({$insertValues})";

        $this->debugLog('Compiled MERGE statement', ['sql' => $sql, 'file' => __FILE__, 'line' => __LINE__]);
        return $sql;
    }

    /**
     * Compile an update statement into SQL.
     *
     * @param  \\Illuminate\\Database\\Query\\Builder  $query
     * @param  array  $values
     * @return string
     */
    public function compileUpdate(Builder $query, array $values): string
    {
        // Use base compileUpdate but ensure WHERE clause is generated correctly
        return parent::compileUpdate($query, $values);
    }

    /**
     * Compile a delete statement into SQL.
     *
     * @param  \\Illuminate\\Database\\Query\\Builder  $query
     * @return string
     */
    public function compileDelete(Builder $query): string
    {
        // Use base compileDelete but ensure WHERE clause is generated correctly
        return parent::compileDelete($query);
    }

    /**
     * Compile a truncate table statement into SQL.
     *
     * @param  \\Illuminate\\Database\\Query\\Builder  $query
     * @return array
     */
    public function compileTruncate(Builder $query): array
    {
        $table = $this->wrapTable($query->from);
        return ["truncate table {$table}" => []];
    }

    /**
     * Get the format for database stored dates.
     *
     * @return string
     */
    public function getDateFormat(): string
    {
        // Snowflake default is 'YYYY-MM-DD HH24:MI:SS.FF3' but TIMESTAMP_NTZ is common
        return 'Y-m-d H:i:s.u'; // Use microseconds for broader compatibility
    }

    /**
     * Prepare the bindings for an update statement.
     *
     * @param array $bindings
     * @param array $values
     * @return array
     */
    public function prepareBindingsForUpdate(array $bindings, array $values): array
    {
        // Snowflake doesn't typically use bindings in the same way due to the API approach.
        // This method might not be directly used if the Connection class builds the full SQL.
        // However, if bindings were to be used, they would be merged here.
        $this->debugLog('prepareBindingsForUpdate (potentially unused)', ['bindings' => $bindings, 'values' => $values]);
        return parent::prepareBindingsForUpdate($bindings, $values);
    }

    /**
     * Quote the given string literal.
     * This is primarily for string values.
     *
     * @param string|array $value
     * @return string
     */
    public function quoteString($value): string
    {
        if (is_array($value)) {
            return implode(', ', array_map([$this, __FUNCTION__], $value));
        }

        // Basic single quote escaping for SQL strings
        return "'" . str_replace("'", "''", (string) $value) . "'";
    }

    protected static function debugLog($message, array $context = []): void
    {
        if (env('SF_DEBUG', false)) {
            Log::info($message, $context);
        }
    }
}
