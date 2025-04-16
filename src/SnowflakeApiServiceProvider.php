<?php

namespace LaravelSnowflakeApi;

use Illuminate\Database\Connection;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\ServiceProvider;
use LaravelSnowflakeApi\Flavours\Snowflake\Grammars\QueryGrammar;
use LaravelSnowflakeApi\Flavours\Snowflake\Grammars\SchemaGrammar;
use LaravelSnowflakeApi\Flavours\Snowflake\Processor;

class SnowflakeApiServiceProvider extends ServiceProvider
{
    /**
     * Register the service provider.
     *
     * @return void
     */
    public function register()
    {
        // Register grammar and processor classes to prevent circular dependencies
        $this->app->bind(QueryGrammar::class, function () {
            return new QueryGrammar();
        });
        
        $this->app->bind(SchemaGrammar::class, function () {
            return new SchemaGrammar();
        });
        
        $this->app->bind(Processor::class, function () {
            return new Processor();
        });
        
        // Register connection resolver with application instance access
        $app = $this->app;
        
        Connection::resolverFor('snowflake_api', function ($connection, $database, $prefix, $config) use ($app) {
            $conn = new SnowflakeApiConnection($connection, $database, $prefix, $config);
            $conn->setApplication($app);
            return $conn;
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
