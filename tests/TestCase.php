<?php

namespace Tests;

use Orchestra\Testbench\TestCase as BaseTestCase;
use LaravelSnowflakeApi\SnowflakeApiServiceProvider;

abstract class TestCase extends BaseTestCase
{
    /**
     * Setup the test environment.
     *
     * @return void
     */
    protected function setUp(): void
    {
        parent::setUp();

        // Environment variable check moved to Integration test setup.
    }
    
    /**
     * Define environment setup.
     *
     * @param  \Illuminate\Foundation\Application  $app
     * @return void
     */
    protected function getEnvironmentSetUp($app)
    {
        // Setup default database connection to be snowflake
        $app['config']->set('database.default', 'snowflake');

        // Setup Snowflake connection details from environment variables
        $app['config']->set('database.connections.snowflake', [
            'driver'                 => 'snowflake_api', // Use the custom driver name
            'host'                   => env('SNOWFLAKE_TEST_URL'), // Use SNOWFLAKE_TEST_URL
            'account'                => env('SNOWFLAKE_TEST_ACCOUNT'), // Use SNOWFLAKE_TEST_ACCOUNT
            'username'               => env('SNOWFLAKE_TEST_USER'), // Use SNOWFLAKE_TEST_USER
            'password'               => env('SNOWFLAKE_PASSWORD'), // Optional, if using key pair (Keep non-TEST version? Check .env)
            'public_key'             => env('SNOWFLAKE_TEST_PUBLIC_KEY'), // Use SNOWFLAKE_TEST_PUBLIC_KEY
            'private_key'            => env('SNOWFLAKE_TEST_PRIVATE_KEY'), // Use SNOWFLAKE_TEST_PRIVATE_KEY
            'private_key_passphrase' => env('SNOWFLAKE_TEST_PASSPHRASE'), // Use SNOWFLAKE_TEST_PASSPHRASE
            'warehouse'              => env('SNOWFLAKE_TEST_WAREHOUSE'), // Use SNOWFLAKE_TEST_WAREHOUSE
            'database'               => env('SNOWFLAKE_TEST_DATABASE'), // Use SNOWFLAKE_TEST_DATABASE
            'schema'                 => env('SNOWFLAKE_TEST_SCHEMA'), // Use SNOWFLAKE_TEST_SCHEMA
            'timeout'                => env('SNOWFLAKE_TIMEOUT', 30), // Keep non-TEST version? Check .env
            'prefix'                 => '', // No prefix usually needed
        ]);

        // Set debug logging based on env variable
        $app['config']->set('snowflake.debug_logging', env('SF_DEBUG', false));
    }
    
    /**
     * Get package providers.
     *
     * @param  \Illuminate\Foundation\Application  $app
     * @return array<int, class-string>
     */
    protected function getPackageProviders($app): array
    {
        return [
            SnowflakeApiServiceProvider::class,
        ];
    }
    
    /**
     * Set a protected property on an object using reflection
     *
     * @param object $object The object to set the property on
     * @param string $property The property name
     * @param mixed $value The value to set
     * @return void
     */
    protected function setPrivateProperty(object $object, string $property, mixed $value): void
    {
        $reflection = new \ReflectionClass($object);
        $property = $reflection->getProperty($property);
        $property->setValue($object, $value);
    }
    
    /**
     * Get a protected property value using reflection
     *
     * @param object $object The object to get the property from
     * @param string $property The property name
     * @return mixed The property value
     */
    protected function getPrivateProperty(object $object, string $property): mixed
    {
        $reflection = new \ReflectionClass($object);
        $property = $reflection->getProperty($property);
        return $property->getValue($object);
    }
} 