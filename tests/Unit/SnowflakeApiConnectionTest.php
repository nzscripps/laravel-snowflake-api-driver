<?php

namespace Tests\Unit;

use Illuminate\Database\Connection as BaseConnection;
use LaravelSnowflakeApi\Flavours\Snowflake\Grammars\QueryGrammar;
use LaravelSnowflakeApi\Flavours\Snowflake\Grammars\SchemaGrammar;
use LaravelSnowflakeApi\Services\SnowflakeService;
use LaravelSnowflakeApi\SnowflakeApiConnection;
use Mockery;
use Tests\TestCase;

class SnowflakeApiConnectionTest extends TestCase
{
    protected function tearDown(): void
    {
        Mockery::close();
        parent::tearDown();
    }

    public function test_get_default_query_grammar_in_laravel12()
    {
        // Create a minimal mock connection
        $connection = new class extends SnowflakeApiConnection
        {
            public function __construct()
            {
                // Override constructor to avoid actual connection setup
            }

            // Make the protected method public for testing
            public function getDefaultQueryGrammarTest()
            {
                return $this->getDefaultQueryGrammar();
            }
        };

        // Manually invoke getDefaultQueryGrammar using our test accessor
        $grammar = $connection->getDefaultQueryGrammarTest();

        // Verify we got a properly configured grammar back
        $this->assertInstanceOf(QueryGrammar::class, $grammar);
        $this->assertEquals('', $grammar->getTablePrefix());
    }

    public function test_get_default_schema_grammar_in_laravel12()
    {
        // Create a minimal mock connection
        $connection = new class extends SnowflakeApiConnection
        {
            public function __construct()
            {
                // Override constructor to avoid actual connection setup
            }

            // Make the protected method public for testing
            public function getDefaultSchemaGrammarTest()
            {
                return $this->getDefaultSchemaGrammar();
            }
        };

        // Manually invoke getDefaultSchemaGrammar using our test accessor
        $grammar = $connection->getDefaultSchemaGrammarTest();

        // Verify we got a properly configured grammar back
        $this->assertInstanceOf(SchemaGrammar::class, $grammar);
        $this->assertEquals('', $grammar->getTablePrefix());
    }

    public function test_query_builder_get_and_cursor_return_case_insensitive_rows(): void
    {
        $connection = new class extends SnowflakeApiConnection
        {
            public function __construct()
            {
                BaseConnection::__construct(new \stdClass, 'test_database');
            }
        };

        $service = Mockery::mock(SnowflakeService::class);
        $service->shouldReceive('executeQueryMapped')->once()->with('select ID from ORDERS', Mockery::type('callable'))
            ->andReturnUsing(fn ($sql, $map) => [$map(['ID' => 42])]);
        $service->shouldReceive('cursor')->once()->with('select ID from ORDERS')
            ->andReturnUsing(function () {
                yield ['ID' => 42];
            });
        $this->setPrivateProperty($connection, 'snowflakeService', $service);

        $this->assertSame(42, $connection->table('orders')->get(['ID'])->first()->id);
        $this->assertSame(42, $connection->table('orders')->select('ID')->cursor()->first()->id);
    }

    public function test_with_table_prefix_on_schema_grammar()
    {
        // Create test prefix
        $prefix = 'test_prefix_';

        // Create our connection with mocked internals
        $connection = new class extends SnowflakeApiConnection
        {
            public function __construct()
            {
                // Override constructor to avoid actual connection setup
            }

            // Mock tablePrefix for testing
            public $tablePrefix = '';
        };

        // Create a schema grammar instance
        $grammar = new SchemaGrammar;

        // Apply the prefix
        $result = $connection->withTablePrefix($grammar);

        // Test that the grammar was returned
        $this->assertSame($grammar, $result);

        // Now set a prefix and test again
        $connection->tablePrefix = $prefix;
        $result = $connection->withTablePrefix($grammar);

        // Verify the grammar has the correct prefix
        $this->assertEquals($prefix, $grammar->getTablePrefix());
    }
}
