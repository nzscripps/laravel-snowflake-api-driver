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
SNOWFLAKE_COLUMNS_CASE_SENSITIVE=false
```

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
