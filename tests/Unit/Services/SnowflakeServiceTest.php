<?php

namespace Tests\Unit\Services;

use Illuminate\Support\Facades\Log;
use LaravelSnowflakeApi\Services\SnowflakeService;
use LaravelSnowflakeApi\Services\ThreadSafeTokenProvider;
use Mockery;
use Tests\TestCase;

class SnowflakeServiceTest extends TestCase
{
    private $service;

    private $mockHttpClient;

    private static $throwawayPrivateKey;

    /**
     * An RSA key generated at runtime and never written to disk.
     *
     * This used to read tests/fixtures/test_private_key.pem, but that file is
     * gitignored (private keys must not be committed), so it only ever existed
     * on one developer's machine and the test errored everywhere else — CI
     * included. setUp() swaps the token provider for a stub, so the key's
     * content is never used to sign anything.
     */
    private static function throwawayPrivateKey(): string
    {
        if (self::$throwawayPrivateKey === null) {
            $key = openssl_pkey_new([
                'private_key_bits' => 2048,
                'private_key_type' => OPENSSL_KEYTYPE_RSA,
            ]);

            if ($key === false || ! openssl_pkey_export($key, $pem)) {
                throw new \RuntimeException('Could not generate a throwaway RSA key: '.openssl_error_string());
            }

            self::$throwawayPrivateKey = $pem;
        }

        return self::$throwawayPrivateKey;
    }

    protected function setUp(): void
    {
        parent::setUp();

        // Mock Log facade to avoid actual logging
        Log::shouldReceive('debug')->byDefault();
        Log::shouldReceive('error')->byDefault();
        Log::shouldReceive('warning')->byDefault();

        // Create the service with fake credentials and a throwaway private key
        $this->service = new SnowflakeService(
            'test-url',
            'test-account',
            'test-user',
            'test-public-key',
            self::throwawayPrivateKey(),
            'test-passphrase',
            'test-warehouse',
            'test-database',
            'test-schema',
            30
        );

        // Mock the HTTP client
        $this->mockHttpClient = Mockery::mock('Symfony\Contracts\HttpClient\HttpClientInterface');

        // Use reflection to inject the mock HTTP client. getHttpClient() swaps
        // in a real client once the current one is older than its max age, so
        // stamp the injected one as fresh.
        $this->setPrivateProperty($this->service, 'httpClient', $this->mockHttpClient);
        $this->setPrivateProperty($this->service, 'httpClientCreatedAt', time());

        // Also disable debug logging
        $this->setPrivateProperty($this->service, 'isDebugEnabled', false);

        // Replace the token provider so no JWT is ever generated or cached
        $tokenProvider = Mockery::mock(ThreadSafeTokenProvider::class);
        $tokenProvider->shouldReceive('getToken')->andReturn('fake-jwt-token');
        $this->setPrivateProperty($this->service, 'tokenProvider', $tokenProvider);
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
            'statementStatusUrl' => 'test-url',
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
                    ['name' => 'col1', 'type' => 'TEXT'],
                ],
            ],
            'createdOn' => time(),
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
