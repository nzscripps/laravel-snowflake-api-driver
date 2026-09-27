# Laravel Snowflake API Driver

This package provides a Laravel Eloquent driver for Snowflake using the Snowflake REST API instead of PDO/ODBC connections.

## Installation

```bash
composer require scripps/laravel-snowflake-api-driver
```

## Configuration

Add the following configuration to your `config/database.php` file:

```php
'connections' => [
    // ... other connections
    
    'snowflake_api' => [
        'driver' => 'snowflake_api',
        'host' => env('SNOWFLAKE_HOST'),
        'account' => env('SNOWFLAKE_ACCOUNT'),
        'username' => env('SNOWFLAKE_USERNAME'),
        'public_key' => env('SNOWFLAKE_PUBLIC_KEY'),
        'private_key' => env('SNOWFLAKE_PRIVATE_KEY'),
        'private_key_passphrase' => env('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE'),
        'warehouse' => env('SNOWFLAKE_WAREHOUSE'),
        'database' => env('SNOWFLAKE_DATABASE'),
        'schema' => env('SNOWFLAKE_SCHEMA'),
        'timeout' => env('SNOWFLAKE_TIMEOUT', 30),
        'cache_driver' => env('SNOWFLAKE_CACHE_DRIVER'),
        'quote_identifiers' => env('SNOWFLAKE_QUOTE_IDENTIFIERS', false),
    ],
],
```

Add the following to your `.env` file:

```
SNOWFLAKE_HOST=your-snowflake-host
SNOWFLAKE_ACCOUNT=your-snowflake-account
SNOWFLAKE_USERNAME=your-snowflake-username
SNOWFLAKE_PUBLIC_KEY=your-snowflake-public-key
SNOWFLAKE_PRIVATE_KEY=your-snowflake-private-key
SNOWFLAKE_PRIVATE_KEY_PASSPHRASE=your-snowflake-private-key-passphrase
SNOWFLAKE_WAREHOUSE=your-snowflake-warehouse
SNOWFLAKE_DATABASE=your-snowflake-database
SNOWFLAKE_SCHEMA=your-snowflake-schema
SNOWFLAKE_TIMEOUT=30
SNOWFLAKE_CACHE_DRIVER=redis
SNOWFLAKE_COLUMNS_CASE_SENSITIVE=false
SNOWFLAKE_QUOTE_IDENTIFIERS=false
```

`cache_driver` is optional. When set, Snowflake token caching and lock coordination use that Laravel cache store instead of the app-wide `cache.default`.

### Quoted identifiers (`quote_identifiers`)

Off by default. When enabled, the query grammar emits **aliases** as double-quoted
Snowflake identifiers:

| Builder call | `quote_identifiers` off (default) | on |
| --- | --- | --- |
| `->select(['DEMO_MAPPING as Demo'])` | `select DEMO_MAPPING as Demo` | `select DEMO_MAPPING as "Demo"` |
| `->from('orders as o')` | `from ORDERS as 'O'` | `from ORDERS as "O"` |

Two things change as a result:

1. **Alias case survives the round trip.** Snowflake folds unquoted identifiers to
   upper case, so `as Demo` comes back as `DEMO` in
   `resultSetMetaData.rowType` — and therefore as an UPPERCASE array key. Quoting
   the alias makes the hydrated key exactly `Demo`.
2. **Table aliases become identifiers instead of string literals.** `as 'O'` is
   Snowflake *string literal* syntax; it is tolerated in a `FROM` clause but is not
   what you meant. With the flag on you no longer need `DB::raw()` to work around
   alias quoting on table names.

#### Exactly what changes, and what does not

Only aliases in a SELECT column list, and table aliases, are quoted. Everything
else is emitted exactly as before, in both flag states:

| Position | Quoted with the flag on? |
| --- | --- |
| `->select(['X as Demo'])` | **yes** — `X as "Demo"` |
| `->from('t as x')`, `->join('t as x', …)` | **yes** — and the alias is upper-cased first by `resolveTableName()` |
| Column *references* (`->where`, `->orderBy`, `->groupBy`, `->having`) | no |
| `->selectSub($q, 'MyCount')`, `->fromSub`, `->joinSub` | no — these route through `wrap()`, which cannot tell an alias from a column |
| `selectRaw` / `DB::raw(...)` | no — raw expressions are never rewritten |
| Insert/upsert/truncate column lists, `groupBy` lists | no — only SELECT column lists get alias handling |

> **Write your aliases in UPPER CASE if anything references them.**
> Because references are not quoted, a mixed-case alias does not round-trip to its
> own reference: `->select(['SUM(x) as Total'])->orderBy('Total')` compiles to
> `select SUM(x) as "Total" … order by Total`, and Snowflake folds the unquoted
> `Total` to `TOTAL` → **invalid identifier**. The same applies to an outer query
> referencing an alias defined in a subquery. An UPPERCASE alias is the one
> spelling that yields the same identifier in both flag states.

Aliases the pattern deliberately does not touch (left byte-identical to today, so
they stay unquoted and keep folding): aliases with a trailing space, aliases
containing an escaped quote (`x as "a""b"`), and anything where the trailing token
is not a bare identifier. Whitespace between the expression and `as` is
normalised to a single space when the alias IS quoted.

> **Migration note.** Turning this on changes the array keys your application reads
> back from aliased selects (`DEMO` becomes `Demo`). Before enabling it, sweep code
> that reads rows by literal UPPERCASE keys — note that `CaseInsensitiveRow`
> already tolerates either casing for array/property reads, so the surfaces that
> actually break are `toArray()`/`jsonSerialize()` output (API payloads) and
> Eloquent attribute access, which is case-sensitive.
>
> The flag is read per-connection, but the `SNOWFLAKE_QUOTE_IDENTIFIERS` fallback
> applies to any connection that does not set `quote_identifiers` explicitly. To
> roll out one connection at a time, set the key explicitly on every Snowflake
> connection and leave the env var unset.

`quote_identifiers` is deliberately separate from `SNOWFLAKE_COLUMNS_CASE_SENSITIVE`,
which controls whether *table names* are upper-cased on the way out. Note that with
`SNOWFLAKE_COLUMNS_CASE_SENSITIVE=true` a lower-case table alias is emitted quoted
and exact-case (`as "o"`) while unqualified references to it still fold to `O`; use
matching case in that combination.

## Usage

### Basic Usage

```php
// Configure your model to use the Snowflake connection
namespace App\Models;

use Illuminate\Database\Eloquent\Model;

class SnowflakeModel extends Model
{
    protected $connection = 'snowflake_api';
    protected $table = 'your_table';
    
    // Disable timestamps if not used in your Snowflake table
    public $timestamps = false;
}

// Use the model just like any other Eloquent model
$results = SnowflakeModel::all();
$filtered = SnowflakeModel::where('column', 'value')->get();
```

### Raw Queries

```php
$results = DB::connection('snowflake_api')
    ->select('SELECT * FROM your_table WHERE condition = ?', ['value']);
```

### DML row counts and unknown outcomes

`statement()`, `update()`, `insert()`, and `delete()` are unchanged: they only
report whether the request completed, never a trustworthy row count (this
driver has no PDO statement to read `rowCount()` from).

For a single conditional DML statement — typically an optimistic-concurrency
write like `UPDATE t SET ..., WRITE_VERSION = WRITE_VERSION + 1 WHERE id = ?
AND WRITE_VERSION = ?` — where the caller actually needs to know how many
rows changed, use `affectingStatement()` (returns the total as an `int`, like
Laravel's own PDO-backed drivers) or `dmlStatement()` (returns a
`LaravelSnowflakeApi\Services\DmlResult` with `getInserted()`/`getUpdated()`/
`getDeleted()`/`affectedRows()`):

```php
$affected = DB::connection('snowflake_api')->affectingStatement(
    'UPDATE t SET x = ?, WRITE_VERSION = WRITE_VERSION + 1 WHERE id = ? AND WRITE_VERSION = ?',
    [$newValue, $id, $expectedVersion]
);

if ($affected === 0) {
    // Someone else already changed this row — the WRITE_VERSION precondition failed.
}

$result = DB::connection('snowflake_api')->dmlStatement($sql, $bindings);
$result->getUpdated();   // int
$result->affectedRows(); // int, sum of inserted+updated+deleted
```

Both read Snowflake's own reported counts — preferring the response's `stats`
object, falling back to the statement's documented output row (e.g. "number
of rows updated") — and never guess.

**Unknown outcomes.** Both methods can throw
`LaravelSnowflakeApi\Exceptions\StatementOutcomeUnknownException` (surfaced by
Laravel as a `QueryException` whose `getPrevious()` is this exception) when
the statement was submitted to Snowflake but whether it completed could not
be confirmed — for example a transport failure while polling for the result,
or the driver's own timeout cancelling a statement that may have already
committed. This is distinct from Snowflake actively *refusing* the statement
(an HTTP 4xx SQL error): a refusal means nothing ran and the original
exception propagates unchanged. An unknown outcome means the statement **may
or may not have executed** — callers must treat it as a possible partial
success (typically by re-checking state) and must never assume it is safe to
blindly retry, since the statement's own fate is still unconfirmed.

## Features

- Uses Snowflake REST API for communication
- Supports all standard Eloquent operations
- SQL queries are translated to Snowflake syntax
- No need for PDO or ODBC drivers to be installed
- Optimized for performance with parallel page processing
- Smart caching of authentication tokens

## Performance Optimizations

The driver includes several optimizations to improve performance:

- **Parallel API Requests**: Multiple result pages are fetched concurrently
- **Token Caching**: JWT tokens are cached to reduce authentication overhead
- **Efficient Type Conversion**: Smart mapping of Snowflake types to PHP types
- **Non-blocking Operations**: Improved polling for query results
- **Memory Optimization**: Efficient data structures and processing

For detailed information about optimizations, see [OPTIMIZATIONS.md](OPTIMIZATIONS.md).

## Testing

The package includes a comprehensive test suite. Before running the integration tests, you'll need to set up your test credentials:

1. Copy `.env.testing` to `.env.testing.local` and add your Snowflake test credentials
2. Make sure your Snowflake user has permissions to create/drop tables

Then run the tests:

```bash
# Run all tests (unit tests followed by integration tests)
composer test

# Run only unit tests (no Snowflake credentials needed)
composer test:unit

# Run only integration tests (requires Snowflake credentials)
composer test:integration
```

All test commands will work properly with the necessary environment configuration.

For detailed information about testing, see [TESTING.md](TESTING.md).

## Debug Logging

This package provides debug logging to help troubleshoot issues. Debug logging is disabled by default and must be explicitly enabled:

```
# In your .env file
SNOWFLAKE_DEBUG_LOGGING=true
```

The debug logging is strictly off unless `SNOWFLAKE_DEBUG_LOGGING` is explicitly set to `true`. 
This ensures no unnecessary logging in production environments.

Note: Debug logging is controlled only by the `SNOWFLAKE_DEBUG_LOGGING` environment variable. 
It is not affected by Laravel's `APP_DEBUG` setting or any other configuration values.

## Date and Time Handling

The Snowflake API driver automatically formats date and time values as strings with consistent formats:

- **DATE**: Values are returned as strings in YYYY-MM-DD format (e.g., '2023-01-01')
- **TIME**: Values are returned as strings in HH:MM:SS format (e.g., '12:34:56')
- **TIMESTAMP/DATETIME**: Values are returned as strings in YYYY-MM-DD HH:MM:SS format (e.g., '2023-01-01 12:34:56')

This makes it simple to work with date and time values without needing to handle conversions:

```php
// Example of working with date/time values
$result = DB::select('SELECT * FROM my_table');

// Using date values directly as strings
$dateString = $result[0]->date_column; // '2023-01-01'

// Using time values directly as strings
$timeString = $result[0]->time_column; // '12:34:56'

// Using datetime values directly as strings
$datetimeString = $result[0]->datetime_column; // '2023-01-01 12:34:56'

// You can also use Carbon if you need DateTime functionality
$carbon = \Carbon\Carbon::parse($result[0]->datetime_column);
```

The driver ensures that all date and time values are consistently formatted for ease of use in your application.

## Requirements

- PHP 7.3 or higher
- Laravel 8.0 or higher

## License

This package is open-sourced software licensed under the MIT license. 
