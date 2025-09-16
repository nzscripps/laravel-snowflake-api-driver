<?php

namespace LaravelSnowflakeApi\Flavours\Snowflake\Grammars;

use Illuminate\Database\Connection;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Database\Schema\Grammars\Grammar;
use Illuminate\Support\Fluent;
use Illuminate\Support\Str;
use LaravelSnowflakeApi\Traits\DebugLogging;

class SchemaGrammar extends Grammar
{
    use DebugLogging;

    /**
     * The database connection instance.
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
     * Create a new schema grammar instance.
     *
     * @return void
     */
    public function __construct(?Connection $connection = null)
    {
        // Don't call parent constructor with connection to avoid circular references
        // Just set the connection directly if provided
        if ($connection) {
            $this->connection = $connection;
            $this->tablePrefix = $connection->getTablePrefix();
        }
    }

    /**
     * Set the connection instance.
     *
     * @return $this
     */
    public function setConnection(Connection $connection)
    {
        $this->connection = $connection;

        return $this;
    }

    /**
     * Get the table prefix.
     *
     * @return string
     */
    public function getTablePrefix()
    {
        return $this->tablePrefix;
    }

    /**
     * Set the table prefix in use by the grammar.
     *
     * @param  string  $prefix
     * @return $this
     */
    public function setTablePrefix($prefix)
    {
        $this->tablePrefix = $prefix;

        return $this;
    }

    /**
     * The possible column modifiers.
     *
     * @var list<string>
     */
    protected $modifiers = [
        'Collate', 'Comment', 'Default', 'Increment', 'Nullable', 'StoredAs', 'VirtualAs', 'Srid', // Added standard modifiers
    ];

    /**
     * The possible column serials (used for Increment modifier).
     *
     * @var list<string>
     */
    protected $serials = ['bigInteger', 'integer', 'mediumInteger', 'smallInteger', 'tinyInteger'];

    /**
     * If this Grammar supports schema changes wrapped in a transaction.
     * Snowflake generally supports transactional DDL.
     *
     * @var bool
     */
    protected $transactions = true;

    /**
     * Compile the query to determine the list of tables.
     *
     * @param  string|null  $schema
     */
    public function compileTables($schema = null): string
    {
        $schema = $schema ?? $this->connection->getConfig('schema');
        // Quote the schema name properly
        $schema = $this->quoteString($schema);

        return "show tables in schema {$schema}";
    }

    /**
     * Compile the query to determine the list of views.
     *
     * @param  string|null  $schema
     */
    public function compileViews($schema = null): string
    {
        $schema = $schema ?? $this->connection->getConfig('schema');
        $schema = $this->quoteString($schema);

        return "show views in schema {$schema}";
    }

    /**
     * Compile the query to determine the list of columns for a given table.
     *
     * @param  string|null  $schema
     * @param  string  $table
     */
    public function compileColumns($schema, $table): string
    {
        $schema = $schema ?? $this->connection->getConfig('schema');
        // Quote identifiers
        $schema = $this->quoteString($schema);
        $table = $this->quoteString($table);

        return "show columns in table {$schema}.{$table}";
        // Alternative: Query information_schema.columns for more details if needed
        // return sprintf('select column_name from information_schema.columns where table_schema = %s and table_name = %s order by ordinal_position', $schema, $table);
    }

    /**
     * Compile a create table command.
     *
     * @param  \Illuminate\Database\Connection  $connection  // Added connection parameter
     */
    public function compileCreate(Blueprint $blueprint, Fluent $command, Connection $connection): string // Add return type hint
    {
        $columns = implode(', ', $this->getColumns($blueprint));
        $sql = $blueprint->temporary ? 'create temporary table ' : 'create table ';
        $sql .= $this->wrapTable($blueprint)." ($columns)";

        // Add table comment if specified
        if (isset($blueprint->comment)) {
            $sql .= ' comment = '.$this->quoteString($blueprint->comment);
        }

        return $sql;
    }

    /**
     * Compile an add column command.
     */
    public function compileAdd(Blueprint $blueprint, Fluent $command): string // Add return type hint
    {
        $columns = $this->prefixArray('add column', $this->getColumns($blueprint)); // Use "add column"

        return 'alter table '.$this->wrapTable($blueprint).' '.implode(', ', $columns);
    }

    /**
     * Compile a primary key command.
     */
    public function compilePrimary(Blueprint $blueprint, Fluent $command): string // Add return type hint
    {
        $columns = $this->columnize($command->columns);
        $tableName = $this->wrapTable($blueprint);
        // Constraint name format: table_pk
        $constraint = $this->wrap($blueprint->getTable().'_pk');

        return "alter table {$tableName} add constraint {$constraint} primary key ({$columns})";
    }

    /**
     * Compile a unique key command.
     */
    public function compileUnique(Blueprint $blueprint, Fluent $command): string // Add return type hint
    {
        $columns = $this->columnize($command->columns);
        $tableName = $this->wrapTable($blueprint);
        // Use the provided index name
        $constraint = $this->wrap($command->index);

        return "alter table {$tableName} add constraint {$constraint} unique ({$columns})";
    }

    /**
     * Compile a plain index key command.
     */
    public function compileIndex(Blueprint $blueprint, Fluent $command): string // Add return type hint
    {
        $columns = $this->columnize($command->columns);
        $indexName = $this->wrap($command->index);
        $tableName = $this->wrapTable($blueprint);

        return "create index {$indexName} on {$tableName} ({$columns})";
    }

    /**
     * Compile a drop table command.
     */
    public function compileDrop(Blueprint $blueprint, Fluent $command): string // Add return type hint
    {
        return 'drop table '.$this->wrapTable($blueprint);
    }

    /**
     * Compile a drop table (if exists) command.
     */
    public function compileDropIfExists(Blueprint $blueprint, Fluent $command): string // Add return type hint
    {
        return 'drop table if exists '.$this->wrapTable($blueprint);
    }

    /**
     * Compile a drop column command.
     */
    public function compileDropColumn(Blueprint $blueprint, Fluent $command): string // Add return type hint
    {
        $columns = $this->prefixArray('drop column', $this->wrapArray($command->columns));
        $tableName = $this->wrapTable($blueprint);

        return "alter table {$tableName} ".implode(', ', $columns);
    }

    /**
     * Compile a drop primary key command.
     */
    public function compileDropPrimary(Blueprint $blueprint, Fluent $command): string // Add return type hint
    {
        $constraint = $this->wrap($blueprint->getTable().'_pk');
        $tableName = $this->wrapTable($blueprint);

        return "alter table {$tableName} drop constraint {$constraint}";
    }

    /**
     * Compile a drop unique key command.
     */
    public function compileDropUnique(Blueprint $blueprint, Fluent $command): string // Add return type hint
    {
        $index = $this->wrap($command->index);
        $tableName = $this->wrapTable($blueprint);

        return "alter table {$tableName} drop constraint {$index}";
    }

    /**
     * Compile a drop index command.
     */
    public function compileDropIndex(Blueprint $blueprint, Fluent $command): string // Add return type hint
    {
        $index = $this->wrap($command->index);

        // Snowflake drops index without specifying table name
        return "drop index {$index}";
    }

    /**
     * Compile a drop spatial index command.
     */
    public function compileDropSpatialIndex(Blueprint $blueprint, Fluent $command): string
    {
        // Spatial indexes might not be standard in Snowflake, treat as regular index
        $this->debugLog('compileDropSpatialIndex treated as compileDropIndex', ['index' => $command->index]);

        return $this->compileDropIndex($blueprint, $command);
    }

    /**
     * Compile a drop foreign key command.
     */
    public function compileDropForeign(Blueprint $blueprint, Fluent $command): string
    {
        $index = $this->wrap($command->index);
        $tableName = $this->wrapTable($blueprint);

        return "alter table {$tableName} drop constraint {$index}";
    }

    /**
     * Compile a rename table command.
     */
    public function compileRename(Blueprint $blueprint, Fluent $command): string // Add return type hint
    {
        $from = $this->wrapTable($blueprint);
        $to = $this->wrapTable($command->to);

        return "alter table {$from} rename to {$to}";
    }

    /**
     * Compile a rename index command.
     */
    public function compileRenameIndex(Blueprint $blueprint, Fluent $command): string // Add return type hint
    {
        $from = $this->wrap($command->from);
        $to = $this->wrap($command->to);

        return "alter index {$from} rename to {$to}";
    }

    /**
     * Compile a rename column command.
     *
     * @throws \RuntimeException
     */
    public function compileRenameColumn(Blueprint $blueprint, Fluent $command): string
    {
        $table = $this->wrapTable($blueprint);
        $from = $this->wrap($command->from);
        $to = $this->wrap($command->to);

        return "alter table {$table} rename column {$from} to {$to}";
    }

    /**
     * Compile a change column command into a series of SQL statements.
     *
     *
     * @throws \RuntimeException
     */
    public function compileChange(Blueprint $blueprint, Fluent $command): array|string
    {
        $table = $this->wrapTable($blueprint);
        $changes = [];

        foreach ($this->getColumns($blueprint) as $column) {
            // Compile the definition for the new column state
            $newDefinition = $this->getType($column).$this->addModifiers('', $blueprint, $column);
            $columnName = $this->wrap($column->name);

            // Construct the ALTER TABLE ... MODIFY COLUMN statement
            $changes[] = "alter table {$table} modify column {$columnName} {$newDefinition}";

            // Handle rename separately if needed (Snowflake doesn't combine modify/rename)
            if (isset($column->renameTo)) {
                $from = $this->wrap($column->name);
                $to = $this->wrap($column->renameTo);
                $changes[] = "alter table {$table} rename column {$from} to {$to}";
            }
        }

        return $changes;
    }

    /**
     * Compile a comment command.
     */
    public function compileComment(Blueprint $blueprint, Fluent $command): string
    {
        $table = $this->wrapTable($blueprint);
        $comment = $this->quoteString($command->comment);

        return "comment on table {$table} is {$comment}";
    }

    /**
     * Compile a create view command.
     */
    public function compileCreateView(string $view, string $select, bool $orReplace = false, bool $materialized = false): string
    {
        $replace = $orReplace ? ' or replace' : '';
        $materialized = $materialized ? ' materialized' : ''; // Snowflake uses MATERIALIZED keyword
        $view = $this->wrapTable($view);

        return "create{$replace}{$materialized} view {$view} as {$select}";
    }

    /**
     * Compile a drop view command.
     */
    public function compileDropView(string $view): string
    {
        $view = $this->wrapTable($view);

        return "drop view {$view}";
    }

    /**
     * Compile a drop view (if exists) command.
     */
    public function compileDropViewIfExists(string $view): string
    {
        $view = $this->wrapTable($view);

        return "drop view if exists {$view}";
    }

    /**
     * Wrap a table in keyword identifiers.
     *
     * @param  mixed  $table
     * @param  string|null  $prefix
     */
    public function wrapTable($table, $prefix = null): string
    {
        // Fallback direct implementation without calling QueryGrammar to avoid circular references
        if ($table instanceof Blueprint) {
            $table = $table->getTable();
        }

        $tableName = (string) $table;

        // Apply case sensitivity setting
        if (! env('SNOWFLAKE_COLUMNS_CASE_SENSITIVE', false)) {
            $tableName = Str::upper($tableName);
        }

        // Apply prefix if provided or use the current tablePrefix
        $prefixedTableName = ($prefix ?? $this->tablePrefix).$tableName;

        // Wrap the table name
        return '"'.str_replace('"', '""', $prefixedTableName).'"';
    }

    /**
     * Get the SQL for a nullable column modifier.
     */
    protected function modifyNullable(Blueprint $blueprint, Fluent $column): ?string // Add return type hint
    {
        // Snowflake uses NULL | NOT NULL syntax
        return $column->nullable ? ' null' : ' not null';
    }

    /**
     * Get the SQL for a default column modifier.
     */
    protected function modifyDefault(Blueprint $blueprint, Fluent $column): ?string // Add return type hint
    {
        if (! is_null($column->default)) {
            return ' default '.$this->getDefaultValue($column->default);
        }

        return null;
    }

    /**
     * Get the SQL for an auto-increment column modifier.
     */
    protected function modifyIncrement(Blueprint $blueprint, Fluent $column): ?string // Add return type hint
    {
        if (in_array($column->type, $this->serials) && $column->autoIncrement) {
            // Snowflake uses AUTOINCREMENT or IDENTITY (often preferred)
            // AUTOINCREMENT implies START 1 INCREMENT 1
            return ' autoincrement';
        }

        return null;
    }

    /**
     * Get the SQL for a comment column modifier.
     */
    protected function modifyComment(Blueprint $blueprint, Fluent $column): ?string // Add return type hint
    {
        if (! is_null($column->comment)) {
            // Comments need single quotes for the string literal
            return ' comment '.$this->quoteString($column->comment);
        }

        return null;
    }

    /**
     * Get the SQL for a collation column modifier.
     *
     * @param  \\Illuminate\\Database\\Schema\\Blueprint  $blueprint
     * @param  \\Illuminate\\Support\\Fluent  $column
     */
    protected function modifyCollate(Blueprint $blueprint, Fluent $column): ?string
    {
        if (! is_null($column->collation)) {
            // Snowflake uses COLLATE keyword
            return ' collate \''.$column->collation.'\'';
        }

        return null;
    }

    // Add Srid, StoredAs, VirtualAs modifiers if needed, though often Snowflake-specific
    // protected function modifySrid(Blueprint $blueprint, Fluent $column): ?string { ... }
    // protected function modifyStoredAs(Blueprint $blueprint, Fluent $column): ?string { ... }
    // protected function modifyVirtualAs(Blueprint $blueprint, Fluent $column): ?string { ... }

    /**
     * Create the column definition for a char type.
     */
    protected function typeChar(Fluent $column): string
    {
        return 'char('.($column->length ?? 255).')';
    }

    /**
     * Create the column definition for a string type.
     */
    protected function typeString(Fluent $column): string // Add return type hint
    {
        // VARCHAR is standard, use length if provided
        return 'varchar('.($column->length ?? 255).')';
    }

    /**
     * Create the column definition for a text type.
     */
    protected function typeText(Fluent $column): string // Add return type hint
    {
        // Snowflake uses VARCHAR for text types, often without explicit length limit (defaults to max)
        return 'varchar';
    }

    /**
     * Create the column definition for a medium text type.
     */
    protected function typeMediumText(Fluent $column): string // Add return type hint
    {
        return 'varchar';
    }

    /**
     * Create the column definition for a long text type.
     */
    protected function typeLongText(Fluent $column): string // Add return type hint
    {
        return 'varchar';
    }

    /**
     * Create the column definition for an integer type.
     */
    protected function typeInteger(Fluent $column): string // Add return type hint
    {
        // INTEGER or NUMBER(38, 0)
        return 'integer';
    }

    /**
     * Create the column definition for a big integer type.
     */
    protected function typeBigInteger(Fluent $column): string // Add return type hint
    {
        // BIGINT or NUMBER(38, 0)
        return 'bigint';
    }

    /**
     * Create the column definition for a medium integer type.
     */
    protected function typeMediumInteger(Fluent $column): string
    {
        return 'integer';
    }

    /**
     * Create the column definition for a tiny integer type.
     */
    protected function typeTinyInteger(Fluent $column): string
    {
        return 'integer'; // Snowflake INT covers small ranges
    }

    /**
     * Create the column definition for a small integer type.
     */
    protected function typeSmallInteger(Fluent $column): string
    {
        return 'integer';
    }

    /**
     * Create the column definition for a float type.
     */
    protected function typeFloat(Fluent $column): string // Add return type hint
    {
        // FLOAT or FLOAT4/FLOAT8
        return 'float';
    }

    /**
     * Create the column definition for a double type.
     */
    protected function typeDouble(Fluent $column): string // Add return type hint
    {
        // DOUBLE or DOUBLE PRECISION
        return 'double';
    }

    /**
     * Create the column definition for a real type.
     */
    protected function typeReal(Fluent $column): string
    {
        return 'float';
    }

    /**
     * Create the column definition for a decimal type.
     */
    protected function typeDecimal(Fluent $column): string // Add return type hint
    {
        // NUMBER(p, s) or DECIMAL(p, s)
        $total = $column->total ?? 10;
        $places = $column->places ?? 2;

        return "decimal({$total}, {$places})";
    }

    /**
     * Create the column definition for a boolean type.
     */
    protected function typeBoolean(Fluent $column): string // Add return type hint
    {
        return 'boolean';
    }

    /**
     * Create the column definition for a date type.
     */
    protected function typeDate(Fluent $column): string // Add return type hint
    {
        return 'date';
    }

    /**
     * Create the column definition for a date-time type.
     */
    protected function typeDateTime(Fluent $column): string // Add return type hint
    {
        // TIMESTAMP_NTZ (No Time Zone) is often preferred
        return 'timestamp_ntz'.($column->precision ? '('.$column->precision.')' : '');
    }

    /**
     * Create the column definition for a date-time (with time zone) type.
     */
    protected function typeDateTimeTz(Fluent $column): string
    {
        // TIMESTAMP_TZ (Time Zone)
        return 'timestamp_tz'.($column->precision ? '('.$column->precision.')' : '');
    }

    /**
     * Create the column definition for a time type.
     */
    protected function typeTime(Fluent $column): string // Add return type hint
    {
        return 'time'.($column->precision ? '('.$column->precision.')' : '');
    }

    /**
     * Create the column definition for a time (with time zone) type.
     */
    protected function typeTimeTz(Fluent $column): string
    {
        // Snowflake doesn't have a direct TIME_TZ type. Use TIMESTAMP_TZ?
        $this->debugLog('typeTimeTz mapped to TIMESTAMP_TZ', ['column' => $column->name]);

        return 'timestamp_tz'.($column->precision ? '('.$column->precision.')' : '');
    }

    /**
     * Create the column definition for a timestamp type.
     */
    protected function typeTimestamp(Fluent $column): string // Add return type hint
    {
        // TIMESTAMP_NTZ is generally equivalent
        return 'timestamp_ntz'.($column->precision ? '('.$column->precision.')' : '');
    }

    /**
     * Create the column definition for a timestamp (with time zone) type.
     */
    protected function typeTimestampTz(Fluent $column): string
    {
        return 'timestamp_tz'.($column->precision ? '('.$column->precision.')' : '');
    }

    /**
     * Create the column definition for a year type.
     */
    protected function typeYear(Fluent $column): string
    {
        // No YEAR type, use INTEGER or DATE
        return 'integer';
    }

    /**
     * Create the column definition for a binary type.
     */
    protected function typeBinary(Fluent $column): string
    {
        return 'binary';
    }

    /**
     * Create the column definition for a uuid type.
     */
    protected function typeUuid(Fluent $column): string
    {
        // Often stored as VARCHAR(36)
        return 'varchar(36)';
    }

    /**
     * Create the column definition for a ULID type.
     */
    protected function typeUlid(Fluent $column): string
    {
        // Often stored as VARCHAR(26)
        return 'varchar(26)';
    }

    /**
     * Create the column definition for an IP address type.
     */
    protected function typeIpAddress(Fluent $column): string
    {
        // Store as VARCHAR
        return 'varchar(45)';
    }

    /**
     * Create the column definition for a MAC address type.
     */
    protected function typeMacAddress(Fluent $column): string
    {
        // Store as VARCHAR
        return 'varchar(17)';
    }

    /**
     * Create the column definition for a json type.
     */
    protected function typeJson(Fluent $column): string // Add return type hint
    {
        // Snowflake uses VARIANT for JSON
        return 'variant';
    }

    /**
     * Create the column definition for a jsonb type.
     */
    protected function typeJsonb(Fluent $column): string // Add return type hint
    {
        // VARIANT covers JSONB use cases as well
        return 'variant';
    }

    /**
     * Create the column definition for a geometry type.
     */
    protected function typeGeometry(Fluent $column): string
    {
        return 'geometry';
    }

    /**
     * Create the column definition for a point type.
     */
    protected function typePoint(Fluent $column): string
    {
        return 'point';
    }

    // Add other geometry types (linestring, polygon, etc.) if needed...

    // Add quoteString method if not inherited or needs override
    public function quoteString($value): string
    {
        if (is_array($value)) {
            return implode(', ', array_map([$this, 'quoteString'], $value));
        }

        // Use single quotes for SQL string literals
        return "'".str_replace("'", "''", $value)."'";
    }

    /**
     * Compile the query to determine if a table exists.
     *
     * @param  string|null  $schema
     * @param  string  $table
     */
    public function compileTableExists($schema, $table): string // Re-added method with correct signature
    {
        $schema = $schema ?? $this->connection->getConfig('schema');

        return $this->compileSelectExists(
            'select * from information_schema.tables where table_schema = ? and table_name = ?',
            $schema, $table
        );
    }
}
