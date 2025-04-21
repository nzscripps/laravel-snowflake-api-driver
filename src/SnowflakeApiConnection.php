<?php

namespace LaravelSnowflakeApi;

use Illuminate\Database\Connection;
use Illuminate\Database\Query\Processors\Processor;
use LaravelSnowflakeApi\Services\SnowflakeService;
use LaravelSnowflakeApi\Flavours\Snowflake\Grammars\QueryGrammar;
use LaravelSnowflakeApi\Flavours\Snowflake\Grammars\SchemaGrammar;
use LaravelSnowflakeApi\Flavours\Snowflake\Processor as SnowflakeProcessor;
use LaravelSnowflakeApi\Traits\DebugLogging;
use Illuminate\Support\Facades\Log;
use PDO;
use Closure;
use Exception;
use Illuminate\Database\Grammar as QueryGrammarContract;
use Illuminate\Database\Schema\Grammar as SchemaGrammarContract;
use Illuminate\Database\Events\TransactionBeginning;
use Illuminate\Database\Events\TransactionCommitted;
use Illuminate\Database\Events\TransactionRolledBack;
use Throwable;

class SnowflakeApiConnection extends Connection
{
    use DebugLogging;

    /**
     * @var SnowflakeService
     */
    protected $snowflakeService;

    /**
     * Create a new database connection instance.
     *
     * @param mixed $pdo
     * @param string $database
     * @param string $tablePrefix
     * @param array $config
     * @return void
     */
    public function __construct($pdo, $database = '', $tablePrefix = '', array $config = [])
    {
        $this->debugLog('SnowflakeApiConnection: Initializing with config', [
            'database' => $database,
            'prefix' => $tablePrefix,
            'config_keys' => array_keys($config),
        ]);

        try {
            parent::__construct($pdo, $database, $tablePrefix, $config);

            // Mask sensitive data for logging
            $logConfig = $config;
            if (isset($logConfig['private_key'])) $logConfig['private_key'] = 'REDACTED';
            if (isset($logConfig['private_key_passphrase'])) $logConfig['private_key_passphrase'] = 'REDACTED';
            if (isset($logConfig['password'])) $logConfig['password'] = 'REDACTED';

            $this->debugLog('SnowflakeApiConnection: Creating SnowflakeService with configuration', $logConfig);

            // Instantiate the SnowflakeService
            $this->snowflakeService = new SnowflakeService(
                $config['host'] ?? '',
                $config['account'] ?? '',
                $config['username'] ?? '',
                $config['public_key'] ?? '',
                $config['private_key'] ?? '',
                $config['private_key_passphrase'] ?? '',
                $config['warehouse'] ?? '',
                $config['database'] ?? $database,
                $config['schema'] ?? '',
                $config['timeout'] ?? 30
            );

            $this->debugLog('SnowflakeApiConnection: SnowflakeService created successfully');

            // Test the connection
            try {
                $this->snowflakeService->testConnection();
                $this->debugLog('SnowflakeApiConnection: Connection test successful');
            } catch (Exception $e) {
                Log::error('SnowflakeApiConnection: Connection test failed', [
                    'error' => $e->getMessage(),
                    'trace' => $e->getTraceAsString()
                ]);
                throw $e;
            }
        } catch (Exception $e) {
            Log::error('SnowflakeApiConnection: Error initializing connection', [
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString()
            ]);
            throw $e;
        }
    }

    /**
     * Get the default query grammar instance.
     *
     * @return \Illuminate\Database\Grammar
     */
    protected function getDefaultQueryGrammar(): QueryGrammarContract
    {
        $this->debugLog('SnowflakeApiConnection: Getting default query grammar');
        $grammar = new QueryGrammar;
        $grammar->setConnection($this);
        return $this->withTablePrefix($grammar);
    }

    /**
     * Get the default schema grammar instance.
     *
     * @return \Illuminate\Database\Grammar
     */
    protected function getDefaultSchemaGrammar(): SchemaGrammarContract
    {
        $this->debugLog('SnowflakeApiConnection: Getting default schema grammar');
        $grammar = new SchemaGrammar;
        $grammar->setConnection($this);
        return $this->withTablePrefix($grammar);
    }

    /**
     * Get the default post processor instance.
     *
     * @return \Illuminate\Database\Query\Processors\Processor
     */
    protected function getDefaultPostProcessor(): Processor
    {
        $this->debugLog('SnowflakeApiConnection: Getting default post processor');
        return new SnowflakeProcessor;
    }

    /**
     * Run a select statement against the database.
     *
     * @param string $query
     * @param array $bindings
     * @param bool $useReadPdo
     * @return array
     */
    public function select($query, $bindings = [], $useReadPdo = true): array
    {
        $this->debugLog('SnowflakeApiConnection: Executing select query', [
            'query' => $query,
            'bindings_count' => count($bindings),
            'useReadPdo' => $useReadPdo
        ]);

        return $this->run($query, $bindings, function ($query, $bindings) {
            if ($this->pretending()) {
                $this->debugLog('SnowflakeApiConnection: Pretending to run query');
                return [];
            }

            try {
                // Execute query using the Snowflake API Service
                $this->debugLog('SnowflakeApiConnection: Preparing query for execution');
                $statement = $this->prepareQuery($query, $bindings);

                $this->debugLog('SnowflakeApiConnection: Executing query via SnowflakeService', [
                    'prepared_query' => $statement
                ]);

                $result = $this->snowflakeService->ExecuteQuery($statement);

                $this->debugLog('SnowflakeApiConnection: Query executed successfully', [
                    'result_count' => is_countable($result) ? count($result) : 'non-countable'
                ]);

                $array = $result->toArray();

                $this->debugLog('SnowflakeApiConnection: Converted result to array', [
                    'array_count' => count($array)
                ]);

                return $array;
            } catch (Exception $e) {
                Log::error('SnowflakeApiConnection: Error executing select query', [
                    'query' => $statement ?? $query,
                    'error' => $e->getMessage(),
                    'trace' => $e->getTraceAsString()
                ]);
                throw $e;
            }
        });
    }

    /**
     * Prepare the query for execution.
     *
     * @param string $query
     * @param array $bindings
     * @return string
     */
    private function prepareQuery($query, array $bindings = [])
    {
        $this->debugLog('SnowflakeApiConnection: Preparing query', [
            'original_query' => $query,
            'bindings_count' => count($bindings)
        ]);

        try {
            $query = $this->replaceBindings($query, $bindings);

            $this->debugLog('SnowflakeApiConnection: Query prepared successfully', [
                'prepared_query' => $query
            ]);

            return $query;
        } catch (Exception $e) {
            Log::error('SnowflakeApiConnection: Error preparing query', [
                'query' => $query,
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString()
            ]);
            throw $e;
        }
    }

    /**
     * Replace the binding placeholders with the actual values.
     *
     * @param string $query
     * @param array $bindings
     * @return string
     */
    private function replaceBindings($query, array $bindings = [])
    {
        if (empty($bindings)) {
            return $query;
        }

        $positions = [];
        $offset = 0;
        while (($pos = mb_strpos($query, '?', $offset)) !== false) {
            $positions[] = $pos;
            $offset = $pos + 1;
        }

        $queryParts = [];
        $prevPos = 0;
        foreach ($positions as $i => $pos) {
            $queryParts[] = mb_substr($query, $prevPos, $pos - $prevPos);
            $queryParts[] = $this->formatBinding($bindings[$i] ?? '');
            $prevPos = $pos + 1;
        }
        $queryParts[] = mb_substr($query, $prevPos);

        return implode('', $queryParts);
    }

    private function formatBinding($value): string
    {
        if (is_string($value)) {
            return "'" . str_replace("'", "''", $value) . "'";
        }
        if (is_bool($value)) {
            return $value ? '1' : '0';
        }
        if (is_int($value) || is_float($value)) {
            return (string) $value;
        }
        if (is_null($value)) {
            return 'NULL';
        }
        return (string) $value;
    }

    /**
     * Execute a statement and return the number of affected rows.
     *
     * @param string $query
     * @param array $bindings
     * @return int
     */
    public function statement($query, $bindings = []): bool
    {
        $this->debugLog('SnowflakeApiConnection: Executing statement', [
            'query' => $query,
            'bindings_count' => count($bindings)
        ]);

        return $this->run($query, $bindings, function ($query, $bindings) {
            if ($this->pretending()) {
                $this->debugLog('SnowflakeApiConnection: Pretending to run statement');
                return true;
            }

            try {
                $statement = $this->prepareQuery($query, $bindings);

                $this->debugLog('SnowflakeApiConnection: Executing statement via SnowflakeService', [
                    'prepared_query' => $statement
                ]);

                $result = $this->snowflakeService->ExecuteQuery($statement);

                $count = $result->count();

                $this->debugLog('SnowflakeApiConnection: Statement executed successfully', [
                    'affected_rows' => $count
                ]);

                return $count >= 0;
            } catch (Exception $e) {
                Log::error('SnowflakeApiConnection: Error executing statement', [
                    'query' => $statement ?? $query,
                    'error' => $e->getMessage(),
                    'trace' => $e->getTraceAsString()
                ]);
                throw $e;
            }
        });
    }

    /**
     * Run an insert statement against the database.
     *
     * @param  string  $query
     * @param  array  $bindings
     * @return bool
     */
    public function insert($query, $bindings = []): bool
    {
        $this->debugLog('SnowflakeApiConnection: Executing insert statement', [
            'query' => $query,
            'bindings_count' => count($bindings)
        ]);

        try {
            $result = $this->statement($query, $bindings) > 0;

            $this->debugLog('SnowflakeApiConnection: Insert statement executed', [
                'result' => $result
            ]);

            return $result;
        } catch (Exception $e) {
            Log::error('SnowflakeApiConnection: Error executing insert statement', [
                'query' => $query,
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString()
            ]);
            throw $e;
        }
    }

    /**
     * Run an insert statement with explicit column names
     *
     * @param  string  $table
     * @param  array  $columns
     * @param  array  $values
     * @return bool
     */
    public function insertWithColumns($table, array $columns, array $values): bool
    {
        $this->debugLog('SnowflakeApiConnection: Executing insert with explicit columns', [
            'table' => $table,
            'columns' => $columns,
            'values_count' => count($values)
        ]);

        if (empty($values)) {
            $this->debugLog('SnowflakeApiConnection: No values to insert, returning early');
            return true;
        }

        // Create a new query builder for the insert
        $query = $this->table($table);
        
        // Set the columns property which will be used by the QueryGrammar
        $query->columns = $columns;
        
        // If the values use numeric keys, normalize them to ensure correct order
        $firstRow = reset($values);
        if (is_array($firstRow) && $this->hasNumericKeys($firstRow)) {
            $this->debugLog('SnowflakeApiConnection: Normalizing values with numeric keys');
            $values = array_map(function($row) {
                // Make sure we're dealing with array values to avoid offset issues
                return is_array($row) ? array_values($row) : [$row];
            }, $values);
        }
        
        // Insert the values
        try {
            return $query->insert($values);
        } catch (Exception $e) {
            Log::error('SnowflakeApiConnection: Error executing insert with columns', [
                'table' => $table,
                'columns' => $columns,
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString()
            ]);
            throw $e;
        }
    }
    
    /**
     * Check if an array has numeric keys
     *
     * @param  array  $array
     * @return bool
     */
    protected function hasNumericKeys(array $array)
    {
        $keys = array_keys($array);
        return count(array_filter($keys, 'is_numeric')) === count($keys);
    }

    /**
     * Run an update statement against the database.
     *
     * @param  string  $query
     * @param  array  $bindings
     * @return int
     */
    public function update($query, $bindings = []): int
    {
        $this->debugLog('SnowflakeApiConnection: Executing update statement', [
            'query' => $query,
            'bindings_count' => count($bindings)
        ]);

        try {
            $result = $this->statement($query, $bindings);

            $this->debugLog('SnowflakeApiConnection: Update statement executed', [
                'affected_rows' => $result
            ]);

            return $result;
        } catch (Exception $e) {
            Log::error('SnowflakeApiConnection: Error executing update statement', [
                'query' => $query,
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString()
            ]);
            throw $e;
        }
    }

    /**
     * Run a delete statement against the database.
     *
     * @param  string  $query
     * @param  array  $bindings
     * @return int
     */
    public function delete($query, $bindings = []): int
    {
        $this->debugLog('SnowflakeApiConnection: Executing delete statement', [
            'query' => $query,
            'bindings_count' => count($bindings)
        ]);

        try {
            $result = $this->statement($query, $bindings);

            $this->debugLog('SnowflakeApiConnection: Delete statement executed', [
                'affected_rows' => $result
            ]);

            return $result;
        } catch (Exception $e) {
            Log::error('SnowflakeApiConnection: Error executing delete statement', [
                'query' => $query,
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString()
            ]);
            throw $e;
        }
    }

    /**
     * Get the SnowflakeService instance.
     *
     * @return \LaravelSnowflakeApi\Services\SnowflakeService
     */
    public function getSnowflakeService(): SnowflakeService
    {
        return $this->snowflakeService;
    }

    /**
     * Start a new database transaction.
     *
     * @return void
     * @throws \Exception
     */
    public function beginTransaction(): void
    {
        $this->debugLog('SnowflakeApiConnection: Beginning transaction', [
            'current_level' => $this->transactions,
            'new_level' => $this->transactions + 1
        ]);

        // Increment transaction count
        $this->transactions++;

        $this->fireConnectionEvent('beganTransaction');

        $this->debugLog('SnowflakeApiConnection: Transaction level incremented', [
            'transaction_level' => $this->transactions
        ]);

        // Note: No actual "BEGIN TRANSACTION" sent via API here.
        // Transaction handling is simulated locally.
    }

    /**
     * Commit the active database transaction.
     *
     * @return void
     */
    public function commit(): void
    {
        $this->debugLog('SnowflakeApiConnection: Committing transaction', [
            'transaction_level' => $this->transactions
        ]);

        if ($this->transactions == 1) {
            $this->fireConnectionEvent('committing');
        }

        // Decrement transaction count
        $this->transactions = max(0, $this->transactions - 1);
        
        $this->fireConnectionEvent('committed');

        $this->debugLog('SnowflakeApiConnection: Transaction committed, level decremented', [
            'transaction_level' => $this->transactions
        ]);

        // Note: No actual "COMMIT" sent via API here.
    }

    /**
     * Rollback the active database transaction.
     *
     * @param int|null $toLevel
     * @return void
     * @throws \Exception
     */
    public function rollBack($toLevel = null): void
    {
        // We allow developers to rollback to a certain transaction level. We will verify
        // that this given transaction level is valid before attempting to rollback to
        // that level. If it's not we will just return out and not attempt anything.
        $toLevel = is_null($toLevel)
                    ? $this->transactions - 1
                    : $toLevel;

        if ($toLevel < 0 || $toLevel >= $this->transactions) {
            return;
        }

        $this->debugLog('SnowflakeApiConnection: Rolling back transaction', [
            'from_level' => $this->transactions,
            'to_level' => $toLevel
        ]);

        // Set the transaction level back to the given level
        $this->transactions = $toLevel;

        $this->fireConnectionEvent('rollingBack');

        $this->debugLog('SnowflakeApiConnection: Rollback processed, transaction level set', [
            'transaction_level' => $this->transactions
        ]);

        // Note: No actual "ROLLBACK" sent via API here.
        // This simulation assumes any API calls made within the "transaction"
        // are either atomic or the application handles compensation logic.
    }

    /**
     * Perform a rollback within the database - in the case of Snowflake API, we don't have
     * direct transaction control through the API.
     *
     * @param int $toLevel
     * @return void
     */
    protected function performRollBack($toLevel): void
    {
        $this->debugLog('SnowflakeApiConnection: Performing simulated rollback (no API call)', [
            'to_level' => $toLevel
        ]);
        
        // No actual rollback is performed directly since Snowflake API
        // doesn't provide direct transaction control
    }

    /**
     * Get the PDO connection - overriding to prevent transaction-related errors.
     * The parent Connection class tries to use PDO methods on our $pdo property
     * for transaction management which won't work with an API-based connection.
     *
     * @return mixed
     */
    public function getPdo()
    {
        // We don't have a real PDO instance, but Laravel's transaction
        // management relies on calling PDO methods on this object
        return $this;
    }
    
    /**
     * For compatibility with PDO methods called by Laravel's transaction management,
     * implement a mock inTransaction method that uses our own transaction counter
     *
     * @return bool
     */
    public function inTransaction(): bool
    {
        return $this->transactions > 0;
    }

    /**
     * Get the current transaction level.
     * Added for compatibility with newer Laravel versions.
     *
     * @return int
     */
    public function transactionLevel(): int
    {
        return $this->transactions;
    }

    /**
     * Execute a Closure within a transaction.
     * Override to ensure our simulated transaction logic is used.
     *
     * @param  \Closure  $callback
     * @param  int  $attempts
     * @return mixed
     *
     * @throws \Throwable
     */
    public function transaction(Closure $callback, $attempts = 1): mixed
    {
        for ($a = 1; $a <= $attempts; $a++) {
            $this->beginTransaction();

            // We'll catch all exceptions triggering a rollback callback. Then we will
            // analyze the types of exceptions thrown and decide whether or not to
            // try the transaction again. We'll retry the transaction on deadlocks.
            try {
                $result = $callback($this);

                $this->commit();
            }

            // If we catch an exception we will roll back the transaction Closure's actions
            // and throw the exception again. We will catch any exception here so we
            // can ensure that the transaction is rolled back before any exception.
            catch (Throwable $e) {
                $this->rollBack();

                throw $e;
            }

            return $result;
        }
        // Throw exception if attempts exhausted? Or return null/false?
        // Base implementation might throw LogicException here.
         throw new \LogicException('Transaction attempts exhausted.');
    }
}
