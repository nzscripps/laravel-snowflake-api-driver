<?php

namespace Tests\Integration;

use Tests\TestCase;
use Tests\TestDataManager;
use LaravelSnowflakeApi\Services\SnowflakeService;
use Illuminate\Support\Facades\Log;

class SnowflakeServiceIntegrationTest extends TestCase
{
    use TestDataManager;
    
    private $service;
    
    protected function setUp(): void
    {
        parent::setUp();
        
        // Mock Log facade to reduce noise in tests with all required methods
        Log::shouldReceive('debug')->byDefault();
        Log::shouldReceive('error')->byDefault();
        Log::shouldReceive('warning')->byDefault();
        Log::shouldReceive('info')->byDefault();
        
        // Skip integration tests if environment isn't configured
        $account = getenv('SNOWFLAKE_TEST_ACCOUNT');
        if (empty($account)) {
            $this->markTestSkipped('Snowflake test environment not configured.');
            return;
        }
        
        // Create real service instance with test credentials
        $this->service = new SnowflakeService(
            getenv('SNOWFLAKE_TEST_URL'),
            getenv('SNOWFLAKE_TEST_ACCOUNT'),
            getenv('SNOWFLAKE_TEST_USER'),
            getenv('SNOWFLAKE_TEST_PUBLIC_KEY'),
            getenv('SNOWFLAKE_TEST_PRIVATE_KEY'),
            getenv('SNOWFLAKE_TEST_PASSPHRASE'),
            getenv('SNOWFLAKE_TEST_WAREHOUSE'),
            getenv('SNOWFLAKE_TEST_DATABASE'),
            getenv('SNOWFLAKE_TEST_SCHEMA'),
            30
        );
        
        // Set up test schema and tables
        $this->setupTestData();
    }
    
    protected function tearDown(): void
    {
        // Clean up test data
        if ($this->service) {
            $this->cleanupTestData();
        }
        
        parent::tearDown();
    }
    
    /** @test */
    public function it_executes_simple_query()
    {
        // Skip if service not initialized
        if (!$this->service) {
            $this->markTestSkipped('Service not initialized');
        }
        
        // Act
        $result = $this->service->ExecuteQuery('SELECT 1 as TEST');
        
        // Assert
        $this->assertCount(1, $result);
        $this->assertEquals(1, $result->first()['TEST']);
    }
    
    /** @test */
    public function it_handles_large_result_set()
    {
        // Skip if service not initialized
        if (!$this->service) {
            $this->markTestSkipped('Service not initialized');
        }
        
        // Act
        $result = $this->service->ExecuteQuery('
            SELECT seq4() as NUM 
            FROM TABLE(GENERATOR(ROWCOUNT => 1000))
        ');
        
        // Assert
        $this->assertCount(1000, $result);
    }
    
    /** @test */
    public function it_queries_test_data()
    {
        // Skip if service not initialized
        if (!$this->service) {
            $this->markTestSkipped('Service not initialized');
        }
        
        // Get the test table name
        $testTable = $this->getTestTableName();
        
        // Act - query the unique test table
        $result = $this->service->ExecuteQuery("
            SELECT * FROM {$testTable} ORDER BY id
        ");
        
        // Debug output
        echo "\nTest Query Results from {$testTable}:\n";
        echo "Total rows: " . count($result) . "\n";
        
        // Assert basic data
        $this->assertCount(2, $result);
        $this->assertEquals('test1', $result->first()['STRING_COL']);
        $this->assertTrue($result->first()['BOOL_COL']);
        
        // Assert date format (should be DateTime object with correct format)
        $this->assertInstanceOf(\DateTime::class, $result->first()['DATE_COL']);
        $this->assertEquals('2023-01-01', $result->first()['DATE_COL']->format('Y-m-d'));
        
        // Assert time format (should be DateTime object with correct format)
        $this->assertInstanceOf(\DateTime::class, $result->first()['TIME_COL']);
        $this->assertEquals('12:34:56', $result->first()['TIME_COL']->format('H:i:s'));
        
        // Assert datetime format (should be DateTime object with correct format)
        $this->assertInstanceOf(\DateTime::class, $result->first()['DATETIME_COL']);
        $this->assertEquals('2023-01-01 12:34:56', $result->first()['DATETIME_COL']->format('Y-m-d H:i:s'));
    }
    
    /** @test */
    public function it_formats_date_and_time_values_correctly()
    {
        // Skip if service not initialized
        if (!$this->service) {
            $this->markTestSkipped('Service not initialized');
        }
        
        // Get the test table name
        $testTable = $this->getTestTableName();
        
        // Act - test with explicit formatting
        $result = $this->service->ExecuteQuery("
            SELECT 
                DATE_COL,
                TO_VARCHAR(DATE_COL, 'YYYY-MM-DD') AS DATE_STR,
                TIME_COL,
                TO_VARCHAR(TIME_COL, 'HH24:MI:SS') AS TIME_STR,
                DATETIME_COL,
                TO_VARCHAR(DATETIME_COL, 'YYYY-MM-DD HH24:MI:SS') AS DATETIME_STR
            FROM {$testTable}
            WHERE id = 1
        ");
        
        // Debug output
        echo "\nDate/Time Format Test Results:\n";
        print_r($result->first());
        
        // Assert
        $firstRow = $result->first();
        
        // Date assertions
        $this->assertInstanceOf(\DateTime::class, $firstRow['DATE_COL']);
        $this->assertEquals('2023-01-01', $firstRow['DATE_STR']);
        $this->assertEquals('2023-01-01', $firstRow['DATE_COL']->format('Y-m-d'));
        
        // Time assertions
        $this->assertInstanceOf(\DateTime::class, $firstRow['TIME_COL']);
        $this->assertEquals('12:34:56', $firstRow['TIME_STR']);
        $this->assertEquals('12:34:56', $firstRow['TIME_COL']->format('H:i:s'));
        
        // DateTime assertions
        $this->assertInstanceOf(\DateTime::class, $firstRow['DATETIME_COL']);
        $this->assertEquals('2023-01-01 12:34:56', $firstRow['DATETIME_STR']);
        $this->assertEquals('2023-01-01 12:34:56', $firstRow['DATETIME_COL']->format('Y-m-d H:i:s'));
    }
} 