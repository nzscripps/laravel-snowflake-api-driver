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
        
        // Assert
        $this->assertCount(2, $result);
        $this->assertEquals('test1', $result->first()['STRING_COL']);
        $this->assertTrue($result->first()['BOOL_COL']);
        $this->assertInstanceOf(\DateTime::class, $result->first()['DATE_COL']);
    }
} 