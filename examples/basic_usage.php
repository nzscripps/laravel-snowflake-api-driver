<?php

/**
 * Basic usage example for Laravel Snowflake API Driver
 *
 * This file demonstrates how you would use the driver in a real Laravel application.
 * Note: This is not meant to be executed directly as it depends on the Laravel environment.
 */

// -----------------------------------------------------------------
// 1. Configuration in config/database.php
// -----------------------------------------------------------------

// First, add the Snowflake connection to your database configuration:
$databaseConfig = [
    'connections' => [
        'snowflake' => [
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
            'prefix' => '',
        ],
    ],
];

// -----------------------------------------------------------------
// 2. Register Service Provider in config/app.php
// -----------------------------------------------------------------

$appConfig = [
    'providers' => [
        // Other providers...
        LaravelSnowflakeApi\SnowflakeApiServiceProvider::class,
    ],
];

// -----------------------------------------------------------------
// 3. Basic Usage in Your Controllers or Services
// -----------------------------------------------------------------

// Example usage in a controller method or other service:
function exampleUsage()
{
    // Get the Snowflake connection
    $snowflake = DB::connection('snowflake');

    // Run a simple query
    $results = $snowflake->select('SELECT * FROM my_table WHERE active = ?', [true]);

    // Use the query builder
    $users = $snowflake->table('users')
        ->where('status', 'active')
        ->where('last_login', '>', now()->subDays(30))
        ->get();

    // Insert records
    $snowflake->table('orders')->insert([
        'user_id' => 1,
        'total' => 99.99,
        'status' => 'pending',
    ]);

    // Update records
    $snowflake->table('orders')
        ->where('id', 1234)
        ->update(['status' => 'shipped']);

    // Use transactions
    $snowflake->transaction(function ($connection): void {
        $connection->table('inventory')->where('product_id', 5)->decrement('stock', 1);
        $connection->table('sales')->insert(['product_id' => 5, 'quantity' => 1]);
    });

    // Execute raw statements
    $snowflake->statement('CREATE TABLE new_table (id NUMBER, name VARCHAR)');
}

// -----------------------------------------------------------------
// 4. Usage with Models
// -----------------------------------------------------------------

/**
 * Define a model that uses the Snowflake connection
 */
class SnowflakeModel extends \Illuminate\Database\Eloquent\Model
{
    // Specify the connection
    protected $connection = 'snowflake';

    // Table name
    protected $table = 'my_snowflake_table';

    // Other model configuration...
    protected $fillable = ['name', 'value', 'active'];
}

// Then use the model in your application
function modelUsage()
{
    // Create
    $record = SnowflakeModel::create([
        'name' => 'Test Record',
        'value' => 123.45,
        'active' => true,
    ]);

    // Read
    $records = SnowflakeModel::where('active', true)->get();

    // Update
    SnowflakeModel::where('id', 1)->update(['value' => 99.99]);

    // Delete
    SnowflakeModel::destroy(1);
}
