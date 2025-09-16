<?php

namespace Tests\Integration;

use Dotenv\Dotenv;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use Tests\TestCase;

class BasicApplicationUsageTest extends TestCase
{
    protected function setUp(): void
    {
        parent::setUp();

        // Load the .env.testing.local file
        if (file_exists(__DIR__.'/../../.env.testing.local')) {
            $dotenv = Dotenv::createImmutable(__DIR__.'/../../', '.env.testing.local');
            $dotenv->load();
        }

        // Mock Log facade to reduce noise in tests
        Log::shouldReceive('debug')->byDefault();
        Log::shouldReceive('error')->byDefault();
        Log::shouldReceive('info')->byDefault();

        // Skip if Snowflake environment isn't configured
        if (empty(env('SNOWFLAKE_TEST_ACCOUNT'))) {
            $this->markTestSkipped('Snowflake test environment not configured.');

            return;
        }
    }

    /**
     * Test basic connection and query execution, just like a regular application would do
     *
     * @return void
     */
    public function test_basic_connection_and_query()
    {
        // This is how a typical Laravel application would access the driver
        $connection = DB::connection('snowflake');

        // Verify we got the right connection type
        $this->assertInstanceOf(\LaravelSnowflakeApi\SnowflakeApiConnection::class, $connection);

        // Create a test table
        $tableName = 'test_table_'.uniqid();
        $connection->statement("CREATE TABLE {$tableName} (id NUMBER, name VARCHAR)");

        // Insert data using builder
        $result = $connection->table($tableName)->insert([
            'id' => 1,
            'name' => 'Test Record',
        ]);

        $this->assertTrue($result);

        // Query data using the query builder
        $records = $connection->table($tableName)->get();

        $this->assertCount(1, $records);
        $this->assertEquals(1, $records[0]->ID);
        $this->assertEquals('Test Record', $records[0]->NAME);

        // Clean up
        $connection->statement("DROP TABLE IF EXISTS {$tableName}");
    }

    /**
     * Test query builder functionality with where clauses
     *
     * @return void
     */
    public function test_query_builder_with_conditions()
    {
        // Get the connection
        $connection = DB::connection('snowflake');

        // Create a test table
        $tableName = 'test_table_'.uniqid();
        $connection->statement("CREATE TABLE {$tableName} (id NUMBER, name VARCHAR, active BOOLEAN)");

        // Insert multiple records
        $connection->table($tableName)->insert([
            ['id' => 1, 'name' => 'Record One', 'active' => true],
            ['id' => 2, 'name' => 'Record Two', 'active' => false],
            ['id' => 3, 'name' => 'Record Three', 'active' => true],
        ]);

        // Query with where clause
        $activeRecords = $connection->table($tableName)
            ->where('active', true)
            ->get();

        $this->assertCount(2, $activeRecords);

        // Query with complex where clause
        $filtered = $connection->table($tableName)
            ->where('id', '>', 1)
            ->where('active', true)
            ->first();

        $this->assertNotNull($filtered);
        $this->assertEquals(3, $filtered->ID);
        $this->assertEquals('Record Three', $filtered->NAME);

        // Clean up
        $connection->statement("DROP TABLE IF EXISTS {$tableName}");
    }

    /**
     * Test transactions as a typical application would use them
     *
     * @return void
     */
    public function test_transactions()
    {
        $connection = DB::connection('snowflake');

        // Create a test table
        $tableName = 'test_table_'.uniqid();
        $connection->statement("CREATE TABLE {$tableName} (id NUMBER, name VARCHAR)");

        // Use transaction - this is a common pattern in Laravel apps
        $connection->transaction(function ($conn) use ($tableName): void {
            $conn->table($tableName)->insert(['id' => 1, 'name' => 'Transaction Test']);
            $conn->table($tableName)->where('id', 1)->update(['name' => 'Updated in Transaction']);
        });

        // Verify transaction was committed
        $record = $connection->table($tableName)->find(1);
        $this->assertNotNull($record);
        $this->assertEquals('Updated in Transaction', $record->NAME);

        // Clean up
        $connection->statement("DROP TABLE IF EXISTS {$tableName}");
    }

    /**
     * Test complex query with PIVOT and quoted identifiers that works in Snowflake client
     * but might break in the Laravel Snowflake API driver
     *
     * @return void
     */
    public function test_complex_pivot_with_quoted_identifiers()
    {
        $connection = DB::connection('snowflake');

        try {
            // Create test tables for the query
            $tableName = 'test_pivot_'.uniqid();
            $connection->statement("
                CREATE TABLE {$tableName} (
                    id NUMBER, 
                    beacon_id VARCHAR, 
                    diff_frac_floor NUMBER
                )
            ");

            // Insert test data
            $connection->table($tableName)->insert([
                ['id' => 1, 'beacon_id' => '9e90t3nOfBJS3oQuFn7MzI3v0G1s', 'diff_frac_floor' => 10],
                ['id' => 2, 'beacon_id' => 'Wzlyy2fuuDSUiTmzhqIq4dGVV1QB', 'diff_frac_floor' => 20],
            ]);

            // Execute a simplified version of the complex query
            $query = "
                WITH test_data AS (
                    SELECT id, beacon_id, diff_frac_floor 
                    FROM {$tableName}
                )
                SELECT 
                    ARRAY_AGG(ID) WITHIN GROUP (ORDER BY ID ASC) AS IDS,
                    ARRAY_AGG(\"'9e90t3nOfBJS3oQuFn7MzI3v0G1s'\") WITHIN GROUP (ORDER BY ID ASC) AS \"9e90t3nOfBJS3oQuFn7MzI3v0G1s\",
                    ARRAY_AGG(\"'Wzlyy2fuuDSUiTmzhqIq4dGVV1QB'\") WITHIN GROUP (ORDER BY ID ASC) AS \"Wzlyy2fuuDSUiTmzhqIq4dGVV1QB\"
                FROM (
                    SELECT 
                        ID,
                        SUM(IFNULL(\"'9e90t3nOfBJS3oQuFn7MzI3v0G1s'\", 0)) AS \"'9e90t3nOfBJS3oQuFn7MzI3v0G1s'\",
                        SUM(IFNULL(\"'Wzlyy2fuuDSUiTmzhqIq4dGVV1QB'\", 0)) AS \"'Wzlyy2fuuDSUiTmzhqIq4dGVV1QB'\"
                    FROM (
                        SELECT ID, beacon_id, diff_frac_floor
                        FROM test_data
                    )
                    PIVOT(SUM(diff_frac_floor) FOR beacon_id IN ('9e90t3nOfBJS3oQuFn7MzI3v0G1s','Wzlyy2fuuDSUiTmzhqIq4dGVV1QB'))
                    GROUP BY ID
                )
            ";

            // Execute and log the result
            $result = $connection->select($query);

            // If we get here without exception, log the result structure
            $this->assertNotNull($result);
            print_r('Query executed successfully. Result: '.json_encode($result));

        } catch (\Exception $e) {
            // This will help determine what's going wrong
            $this->fail('Query execution failed: '.$e->getMessage()."\n".$e->getTraceAsString());
        } finally {
            // Clean up
            $connection->statement("DROP TABLE IF EXISTS {$tableName}");
        }
    }
}
