<?php

namespace Tests\Unit\Services;

use Tests\TestCase;
use LaravelSnowflakeApi\Services\SnowflakeService;
use LaravelSnowflakeApi\Services\Result;
use Illuminate\Support\Facades\Log;
use Mockery;

class SnowflakeServiceTest extends TestCase
{
    private $service;
    private $mockHttpClient;
    
    protected function setUp(): void
    {
        parent::setUp();
        
        // Mock Log facade to avoid actual logging
        Log::shouldReceive('debug')->byDefault();
        Log::shouldReceive('error')->byDefault();
        Log::shouldReceive('warning')->byDefault();
        
        // Ensure the test private key file exists
        $privateKeyPath = __DIR__ . '/../../fixtures/test_private_key.pem';
        if (!file_exists($privateKeyPath)) {
            throw new \Exception("Test private key file not found at: $privateKeyPath");
        }
        
        // Create the service with fake credentials but real private key file
        $this->service = new SnowflakeService(
            'test-url',
            'test-account',
            'test-user',
            'test-public-key',
            file_get_contents($privateKeyPath),
            'test-passphrase',
            'test-warehouse',
            'test-database',
            'test-schema',
            30
        );
        
        // Mock the HTTP client
        $this->mockHttpClient = Mockery::mock('Symfony\Contracts\HttpClient\HttpClientInterface');
        
        // Use reflection to inject the mock HTTP client
        $this->setPrivateProperty($this->service, 'httpClient', $this->mockHttpClient);
        
        // Also disable debug logging
        $this->setPrivateProperty($this->service, 'isDebugEnabled', false);
        
        // Pre-set a fake token to avoid token generation
        $this->setPrivateProperty($this->service, 'cachedToken', 'fake-jwt-token');
        $this->setPrivateProperty($this->service, 'tokenExpiry', time() + 3600);
    }
    
    protected function tearDown(): void
    {
        Mockery::close();
        parent::tearDown();
    }
    
    /** @test */
    public function it_posts_statement_successfully()
    {
        // Arrange
        $query = 'SELECT * FROM test_table';
        $statementId = 'test-statement-id';
        
        $expectedResponse = [
            'statementHandle' => $statementId,
            'code' => '333334', // Async code from the service
            'message' => 'success',
            'statementStatusUrl' => 'test-url'
        ];
        
        $mockResponse = $this->createMockResponse($expectedResponse);
        
        $this->mockHttpClient
            ->shouldReceive('request')
            ->once()
            ->andReturn($mockResponse);
             
        // Act
        $result = $this->service->postStatement($query);
        
        // Assert
        $this->assertEquals($statementId, $result);
    }
    
    /** @test */
    public function it_gets_statement_results()
    {
        // Arrange
        $statementId = 'test-statement-id';
        $responseData = [
            'code' => '090001', // Success code
            'statementHandle' => $statementId,
            'data' => [['col1' => 'value1']],
            'resultSetMetaData' => [
                'numRows' => 1,
                'partitionInfo' => [0],
                'rowType' => [
                    ['name' => 'col1', 'type' => 'TEXT']
                ]
            ],
            'createdOn' => time()
        ];
        
        $mockResponse = $this->createMockResponse($responseData);
        
        $this->mockHttpClient
            ->shouldReceive('request')
            ->once()
            ->andReturn($mockResponse);
            
        // Act
        $result = $this->service->getStatement($statementId, 1);
        
        // Assert
        $this->assertEquals($responseData, $result);
    }
    
    private function createMockResponse(array $data)
    {
        $mockResponse = Mockery::mock('Symfony\Contracts\HttpClient\ResponseInterface');
        
        $mockResponse->shouldReceive('getContent')
            ->andReturn(json_encode($data));
            
        $mockResponse->shouldReceive('getStatusCode')
            ->andReturn(200);
            
        $mockResponse->shouldReceive('getHeaders')
            ->andReturn(['content-type' => ['application/json']]);
            
        $mockResponse->shouldReceive('getInfo')
            ->andReturn('test-url');
            
        return $mockResponse;
    }
} 