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

The package includes a comprehensive test suite:

```bash
# Run all tests
composer test

# Run only unit tests
composer test:unit

# Run only integration tests
composer test:integration
```

For detailed information about testing, see [TESTING.md](TESTING.md).

## Debug Logging

You can enable debug logging to troubleshoot issues:

```php
// In your .env file
SNOWFLAKE_DEBUG_LOGGING=true
```

Or configure it in your application's configuration:

```php
// config/snowflake.php
return [
    'debug_logging' => env('SNOWFLAKE_DEBUG_LOGGING', false),
];
```

## Requirements

- PHP 7.3 or higher
- Laravel 8.0 or higher

## License

This package is open-sourced software licensed under the MIT license. 
