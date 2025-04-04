<?php

namespace Tests;

use Orchestra\Testbench\TestCase as BaseTestCase;
use LaravelSnowflakeApi\SnowflakeApiServiceProvider;
use Dotenv\Dotenv;

class TestCase extends BaseTestCase
{
    /**
     * Setup the test environment.
     *
     * @return void
     */
    protected function setUp(): void
    {
        parent::setUp();
        
        // Load local test environment if available
        $localEnvFile = __DIR__ . '/../.env.testing.local';
        if (file_exists($localEnvFile)) {
            // Create a Dotenv instance for the local file
            try {
                $dotenv = Dotenv::createImmutable(dirname($localEnvFile), basename($localEnvFile));
                $dotenv->safeLoad();
            } catch (\Throwable $e) {
                // Log error but continue with tests
                if (function_exists('logger')) {
                    logger()->error('Error loading .env.testing.local: ' . $e->getMessage());
                }
            }
        }
    }
    
    /**
     * Define environment setup.
     *
     * @param  \Illuminate\Foundation\Application  $app
     * @return void
     */
    protected function getEnvironmentSetUp($app)
    {
        // Setup default database to use sqlite memory
        $app['config']->set('database.default', 'testing');
        $app['config']->set('database.connections.testing', [
            'driver'   => 'sqlite',
            'database' => ':memory:',
            'prefix'   => '',
        ]);
        
        // Setup Snowflake configuration
        $app['config']->set('snowflake.debug_logging', false);
    }
    
    /**
     * Get package providers.
     *
     * @param  \Illuminate\Foundation\Application  $app
     * @return array
     */
    protected function getPackageProviders($app)
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
    protected function setPrivateProperty($object, $property, $value)
    {
        $reflection = new \ReflectionClass($object);
        $property = $reflection->getProperty($property);
        $property->setAccessible(true);
        $property->setValue($object, $value);
    }
    
    /**
     * Get a protected property value using reflection
     *
     * @param object $object The object to get the property from
     * @param string $property The property name
     * @return mixed The property value
     */
    protected function getPrivateProperty($object, $property)
    {
        $reflection = new \ReflectionClass($object);
        $property = $reflection->getProperty($property);
        $property->setAccessible(true);
        return $property->getValue($object);
    }
} 