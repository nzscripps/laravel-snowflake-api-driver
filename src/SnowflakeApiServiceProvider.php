<?php

namespace LaravelSnowflakeApi;

use Illuminate\Database\Connection;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\ServiceProvider;
use LaravelSnowflakeApi\Flavours\Snowflake\Grammars\QueryGrammar;
use LaravelSnowflakeApi\Flavours\Snowflake\Grammars\SchemaGrammar;

class SnowflakeApiServiceProvider extends ServiceProvider
{
    /**
     * Register the service provider.
     *
     * @return void
     */
    public function register()
    {
        // Register grammar classes in the container to avoid circular dependencies
        $this->app->bind(QueryGrammar::class, function ($app) {
            return new QueryGrammar;
        });

        $this->app->bind(SchemaGrammar::class, function ($app) {
            return new SchemaGrammar;
        });

        // Also register with underscore for backwards compatibility
        Connection::resolverFor('snowflake_api', function ($connection, $database, $prefix, $config) {
            return new SnowflakeApiConnection($connection, $database, $prefix, $config);
        });
    }

    /**
     * Bootstrap the application events.
     *
     * @return void
     */
    public function boot()
    {
        Model::setConnectionResolver($this->app['db']);
        Model::setEventDispatcher($this->app['events']);

        // Validate cache driver configuration for thread-safe token management
        $this->validateCacheDriver();
    }

    /**
     * Validate cache driver supports required features
     *
     * This method warns if the configured cache driver doesn't support
     * atomic locks, which are required for optimal thread-safe token management.
     *
     * Supported drivers with atomic locks:
     * - redis (recommended for production)
     * - memcached (acceptable for production)
     * - database (works but may be slow for high-concurrency)
     *
     * Unsupported drivers (will fall back to non-atomic generation):
     * - file (not safe for distributed systems)
     * - array (testing only)
     */
    private function validateCacheDriver(): void
    {
        try {
            $connections = config('database.connections', []);
            $defaultDriver = config('cache.default', 'array');
        } catch (\Exception $e) {
            return;
        }

        $unsafeDrivers = ['file', 'array', 'null'];
        $snowflakeConnections = array_filter(
            $connections,
            static fn ($connection) => ($connection['driver'] ?? null) === 'snowflake_api'
        );

        foreach ($snowflakeConnections as $name => $connection) {
            $driver = $connection['cache_driver'] ?? $defaultDriver;

            if (in_array($driver, $unsafeDrivers, true)) {
                \Illuminate\Support\Facades\Log::warning(
                    'Snowflake API Driver: Cache driver does not support atomic locks',
                    [
                        'connection' => $name,
                        'current_driver' => $driver,
                        'recommended_drivers' => ['redis', 'memcached'],
                        'impact' => 'Token generation may not be atomic under high concurrency',
                        'mitigation' => 'The driver will fall back to non-atomic generation',
                    ]
                );
            }
        }
    }
}
