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
            return new QueryGrammar();
        });
        
        $this->app->bind(SchemaGrammar::class, function ($app) {
            return new SchemaGrammar();
        });
        
        // Register connection resolver
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
    }
}
