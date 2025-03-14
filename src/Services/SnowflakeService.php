<?php

declare(strict_types=1);

namespace LaravelSnowflakeApi\Services;

use LaravelSnowflakeApi\Services\Result;
use Exception;
use Illuminate\Support\Collection;
use Illuminate\Support\Facades\Log;
use Jose\Component\Core\AlgorithmManager;
use Jose\Component\KeyManagement\JWKFactory;
use Jose\Component\Signature\Algorithm\RS256;
use Jose\Component\Signature\JWSBuilder;
use Jose\Component\Signature\Serializer\CompactSerializer;
use Symfony\Component\HttpClient\HttpClient;
use Symfony\Component\HttpClient\Exception\JsonException;
use Symfony\Contracts\HttpClient\ResponseInterface;

class SnowflakeService
{
    private const CODE_SUCCESS = '090001';
    private const CODE_ASYNC = '333334';

    public function __construct(
        private string $baseUrl,
        private string $account,
        private string $user,
        private string $publicKey,
        private string $privateKey,
        private string $privateKeyPassphrase,
        private string $warehouse,
        private string $database,
        private string $schema,
        private int $timeout
    ) {
        Log::info('SnowflakeService: Initialized', [
            'baseUrl' => $this->baseUrl,
            'account' => $this->account,
            'user' => $this->user,
            'warehouse' => $this->warehouse,
            'database' => $this->database,
            'schema' => $this->schema,
            'timeout' => $this->timeout,
            'has_privateKey' => !empty($this->privateKey),
            'has_publicKey' => !empty($this->publicKey),
            'has_passphrase' => !empty($this->privateKeyPassphrase),
        ]);
    }

    /**
     * Test the connection by attempting to generate an access token
     */
    public function testConnection(): void
    {
        Log::info('SnowflakeService: Testing connection');
        try {
            $token = $this->getAccessToken();
            Log::info('SnowflakeService: Connection test successful - token generated');
        } catch (Exception $e) {
            Log::error('SnowflakeService: Connection test failed', [
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString()
            ]);
            throw new Exception("Connection test failed: " . $e->getMessage(), 0, $e);
        }
    }

    public function ExecuteQuery(string $query): Collection
    {
        Log::info('SnowflakeService: Starting query execution', [
            'query' => $query,
        ]);

        try {
            $statementId = $this->postStatement($query);
            Log::info('SnowflakeService: Statement posted', ['statementId' => $statementId]);

            $result = $this->getResult($statementId);
            Log::info('SnowflakeService: Initial result retrieved', [
                'executed' => $result->isExecuted(),
                'id' => $result->getId(),
            ]);

            $startTime = time();
            while (!$result->isExecuted()) {
                $timeElapsed = time() - $startTime;
                Log::info('SnowflakeService: Waiting for execution to complete', [
                    'statementId' => $statementId,
                    'timeElapsed' => $timeElapsed,
                    'timeout' => $this->timeout,
                ]);

                if ($timeElapsed >= $this->timeout) {
                    Log::warning('SnowflakeService: Query execution timed out', [
                        'statementId' => $statementId,
                        'timeout' => $this->timeout,
                    ]);
                    $this->cancelStatement($statementId);
                    return collect();
                }

                sleep(1); // Sleep for 1 second
                $result = $this->getResult($statementId);
                Log::info('SnowflakeService: Checked result status', [
                    'executed' => $result->isExecuted(),
                ]);
            }

            $data = $result->getData();
            Log::info('SnowflakeService: Initial data received', [
                'count' => count($data),
            ]);

            $pageNumber = 1;
            while ($result->getPaginationNext()) {
                $pageNumber++;
                Log::info('SnowflakeService: Retrieving additional page', [
                    'page' => $pageNumber,
                ]);
                $newData = $result->getData();
                Log::info('SnowflakeService: Additional data retrieved', [
                    'page' => $pageNumber,
                    'count' => count($newData),
                ]);
                $data = array_merge($data, $newData);
            }

            $collection = collect($data);
            Log::info('SnowflakeService: Query execution completed', [
                'total_results' => $collection->count(),
            ]);

            return $collection;
        } catch (Exception $e) {
            Log::error('SnowflakeService: Error executing query', [
                'query' => $query,
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString(),
            ]);
            throw $e;
        }
    }

    private function getAccessToken(): string
    {
        Log::info('SnowflakeService: Generating access token');

        try {
            // Check if we have a file path rather than actual key content
            $keyContent = $this->privateKey;
            if (!str_starts_with($keyContent, '-----BEGIN')) {
                if (file_exists($keyContent)) {
                    Log::info('SnowflakeService: Loading private key from file', [
                        'file' => $keyContent,
                    ]);
                    $keyContent = file_get_contents($keyContent);
                    Log::info('SnowflakeService: Private key loaded from file', [
                        'key_length' => strlen($keyContent),
                    ]);
                } else {
                    Log::error('SnowflakeService: Private key file not found', [
                        'file' => $keyContent,
                    ]);
                    throw new Exception("Private key file not found: {$keyContent}");
                }
            }

            Log::info('SnowflakeService: Creating JWK from private key');
            $privateKey = JWKFactory::createFromKeyFile(
                resource_path('snowflake_private_key.p8'),
                env('SNOWFLAKE_KEY_PHRASE'),
                [
                    'use' => 'sig',
                    'alg' => 'RS256',
                ]
            );
            /*
            $privateKey = \Jose\Component\KeyManagement\JWKFactory::createFromKeyFile(
                resource_path('snowflake_private_key.p8'),
                env('SNOWFLAKE_KEY_PHRASE'),
                [
                    'use' => 'sig',
                    'alg' => 'RS256',
                ]
            );
            */
            Log::info('SnowflakeService: JWK created successfully');

            $publicKeyFingerprint = 'SHA256:' . $this->publicKey;
            $expires_in = time() + (60 * 60);
            $payload = [
                'iss' => sprintf('SCRIPPS.%s.%s', $this->user, $publicKeyFingerprint),
                'sub' => sprintf('SCRIPPS.%s', $this->user),
                'iat' => time(),
                'exp' => $expires_in,
            ];

            Log::info('SnowflakeService: Creating JWT payload', [
                'iss' => $payload['iss'],
                'sub' => $payload['sub'],
                'exp_in_seconds' => $expires_in - time(),
            ]);

            $algorithmManager = new AlgorithmManager([new RS256()]);
            $jwsBuilder = new JWSBuilder($algorithmManager);

            Log::info('SnowflakeService: Building JWS');
            $jws = $jwsBuilder
                ->create()
                ->withPayload(json_encode($payload))
                ->addSignature($privateKey, ['alg' => 'RS256', 'typ' => 'JWT'])
                ->build();
            Log::info('SnowflakeService: JWS built successfully');

            $serializer = new CompactSerializer();
            $access_token = $serializer->serialize($jws);

            Log::info('SnowflakeService: Access token generated successfully', [
                'token_length' => strlen($access_token),
            ]);

            return $access_token;
        } catch (Exception $e) {
            Log::error('SnowflakeService: Error generating access token', [
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString(),
            ]);
            throw new Exception("Failed to generate access token: " . $e->getMessage(), 0, $e);
        }
    }

    public function postStatement(string $statement): string
    {
        Log::info('SnowflakeService: Posting statement', [
            'statement' => $statement,
        ]);

        try {
            $variables = http_build_query([
                'async' => 'true',
                'nullable' => 'true'
            ]);

            $url = sprintf('https://%s.snowflakecomputing.com/api/v2/statements?%s', $this->account, $variables);
            Log::info('SnowflakeService: Prepared URL for statement', [
                'url' => $url,
            ]);

            $data = [
                'statement' => $statement,
                'warehouse' => $this->warehouse,
                'database' => $this->database,
                'schema' => $this->schema,
                'resultSetMetaData' => [
                    'format' => 'jsonv2',
                ],
            ];
            Log::info('SnowflakeService: Prepared data for statement', [
                'warehouse' => $this->warehouse,
                'database' => $this->database,
                'schema' => $this->schema,
            ]);

            Log::info('SnowflakeService: Getting headers for request');
            $headers = $this->getHeaders();
            Log::info('SnowflakeService: Headers retrieved');

            Log::info('SnowflakeService: Creating HTTP client');
            $httpClient = HttpClient::create();

            Log::info('SnowflakeService: Sending POST request');
            $response = $httpClient->request('POST', $url, [
                'headers' => $headers,
                'json' => $data,
            ]);
            Log::info('SnowflakeService: POST request sent');

            Log::info('SnowflakeService: Converting response to array');
            $content = $this->toArray($response);
            Log::info('SnowflakeService: Response converted to array');

            Log::info('SnowflakeService: Validating result');
            $this->hasResult($content, [self::CODE_ASYNC]);
            Log::info('SnowflakeService: Result validated');

            Log::info('SnowflakeService: Statement posted successfully', [
                'statementHandle' => $content['statementHandle'],
            ]);

            return $content['statementHandle'];
        } catch (Exception $e) {
            Log::error('SnowflakeService: Error posting statement', [
                'statement' => $statement,
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString(),
            ]);
            throw $e;
        }
    }

    public function getStatement(string $id, int $page): array
    {
        Log::info('SnowflakeService: Getting statement results', [
            'statementId' => $id,
            'page' => $page,
        ]);

        try {
            $variables = http_build_query([
                'partition' => $page - 1,
            ]);

            $url = sprintf('https://%s.snowflakecomputing.com/api/v2/statements/%s?%s', $this->account, $id, $variables);
            Log::info('SnowflakeService: Prepared URL for getting statement', [
                'url' => $url,
            ]);

            Log::info('SnowflakeService: Getting headers for request');
            $headers = $this->getHeaders();

            Log::info('SnowflakeService: Creating HTTP client');
            $httpClient = HttpClient::create();

            Log::info('SnowflakeService: Sending GET request');
            $response = $httpClient->request('GET', $url, [
                'headers' => $headers,
            ]);
            Log::info('SnowflakeService: GET request sent');

            Log::info('SnowflakeService: Converting response to array');
            $result = $this->toArray($response);

            Log::info('SnowflakeService: Statement results retrieved', [
                'code' => $result['code'] ?? 'no-code',
                'has_data' => isset($result['data']),
                'data_count' => isset($result['data']) ? count($result['data']) : 0,
            ]);

            return $result;
        } catch (Exception $e) {
            Log::error('SnowflakeService: Error getting statement', [
                'statementId' => $id,
                'page' => $page,
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString(),
            ]);
            throw $e;
        }
    }

    public function cancelStatement(string $id): void
    {
        Log::info('SnowflakeService: Cancelling statement', [
            'statementId' => $id,
        ]);

        try {
            $url = sprintf('https://%s.snowflakecomputing.com/api/v2/statements/%s/cancel', $this->account, $id);
            Log::info('SnowflakeService: Prepared URL for cancelling statement', [
                'url' => $url,
            ]);

            Log::info('SnowflakeService: Getting headers for request');
            $headers = $this->getHeaders();

            Log::info('SnowflakeService: Creating HTTP client');
            $httpClient = HttpClient::create();

            Log::info('SnowflakeService: Sending POST request for cancellation');
            $response = $httpClient->request('POST', $url, [
                'headers' => $headers,
            ]);
            Log::info('SnowflakeService: POST request for cancellation sent');

            Log::info('SnowflakeService: Converting response to array and validating');
            $this->hasResult($response->toArray(false), [self::CODE_SUCCESS]);

            Log::info('SnowflakeService: Statement cancelled successfully');
        } catch (Exception $e) {
            Log::error('SnowflakeService: Error cancelling statement', [
                'statementId' => $id,
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString(),
            ]);
            throw $e;
        }
    }

    public function getResult(string $id): Result
    {
        Log::info('SnowflakeService: Getting result for statement', [
            'statementId' => $id,
        ]);

        try {
            Log::info('SnowflakeService: Fetching statement data for page 1');
            $data = $this->getStatement($id, 1);

            $executed = $data['code'] === self::CODE_SUCCESS;
            Log::info('SnowflakeService: Statement execution status', [
                'executed' => $executed,
                'code' => $data['code'],
            ]);

            Log::info('SnowflakeService: Creating Result object');
            $result = new Result($this);
            $result->setId($data['statementHandle']);
            $result->setExecuted($executed);

            if (false === $executed) {
                Log::info('SnowflakeService: Statement not yet executed, returning partial result');
                return $result;
            }

            Log::info('SnowflakeService: Validating response data structure');
            foreach (['resultSetMetaData', 'data', 'createdOn'] as $field) {
                if (false === array_key_exists($field, $data)) {
                    $errorMsg = sprintf('Object "%s" not found', $field);
                    Log::error('SnowflakeService: Required field missing in response', [
                        'field' => $field,
                        'keys_present' => array_keys($data),
                    ]);
                    throw new Exception($errorMsg);
                }
            }

            foreach (['numRows', 'partitionInfo', 'rowType'] as $field) {
                if (false === array_key_exists($field, $data['resultSetMetaData'])) {
                    $errorMsg = sprintf('Object "%s" in "resultSetMetaData" not found', $field);
                    Log::error('SnowflakeService: Required metadata field missing in response', [
                        'field' => $field,
                        'keys_present' => array_keys($data['resultSetMetaData']),
                    ]);
                    throw new Exception($errorMsg);
                }
            }

            Log::info('SnowflakeService: Populating Result object with data', [
                'numRows' => $data['resultSetMetaData']['numRows'],
                'partitionInfo_count' => count($data['resultSetMetaData']['partitionInfo']),
                'data_count' => count($data['data']),
            ]);

            $result->setTotal($data['resultSetMetaData']['numRows']);
            $result->setPage(1);
            $result->setPageTotal(count($data['resultSetMetaData']['partitionInfo']));
            $result->setFields($data['resultSetMetaData']['rowType']);
            $result->setData($data['data']);
            $result->setTimestamp($data['createdOn']);

            Log::info('SnowflakeService: Result object created successfully');
            return $result;
        } catch (Exception $e) {
            Log::error('SnowflakeService: Error getting result', [
                'statementId' => $id,
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString(),
            ]);
            throw $e;
        }
    }

    private function hasResult(array $data, array $codes): void
    {
        Log::info('SnowflakeService: Validating result data', [
            'expected_codes' => $codes,
            'actual_code' => $data['code'] ?? 'not-set',
        ]);

        try {
            foreach (['code', 'message'] as $field) {
                if (false === array_key_exists($field, $data)) {
                    Log::error('SnowflakeService: Required field missing in response', [
                        'field' => $field,
                        'keys_present' => array_keys($data),
                    ]);
                    throw new Exception('Unacceptable result', 406);
                }
            }

            if (false === in_array($data['code'], $codes)) {
                Log::error('SnowflakeService: Unexpected result code', [
                    'expected_codes' => $codes,
                    'actual_code' => $data['code'],
                    'message' => $data['message'],
                ]);
                throw new Exception(sprintf('%s (%s)', $data['message'], $data['code']), 422);
            }

            foreach (['statementHandle', 'statementStatusUrl'] as $field) {
                if (false === array_key_exists($field, $data)) {
                    Log::error('SnowflakeService: Required field missing in response', [
                        'field' => $field,
                        'keys_present' => array_keys($data),
                    ]);
                    throw new Exception('Unprocessable result', 422);
                }
            }

            Log::info('SnowflakeService: Result validation successful');
        } catch (Exception $e) {
            Log::error('SnowflakeService: Result validation failed', [
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString(),
            ]);
            throw $e;
        }
    }

    private function getHeaders(): array
    {
        Log::info('SnowflakeService: Generating request headers');

        try {
            Log::info('SnowflakeService: Getting access token');
            $accessToken = $this->getAccessToken();

            $headers = [
                sprintf('Authorization: Bearer %s', $accessToken),
                'Accept-Encoding: gzip',
                'User-Agent: SnowflakeService/0.5',
                'X-Snowflake-Authorization-Token-Type: KEYPAIR_JWT',
            ];

            Log::info('SnowflakeService: Headers generated successfully', [
                'header_count' => count($headers),
            ]);

            return $headers;
        } catch (Exception $e) {
            Log::error('SnowflakeService: Error generating headers', [
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString(),
            ]);
            throw $e;
        }
    }

    private function toArray(ResponseInterface $response): array
    {
        Log::info('SnowflakeService: Converting HTTP response to array', [
            'status_code' => $response->getStatusCode(),
            'content_type' => $response->getHeaders(false)['content-type'][0] ?? 'unknown',
        ]);

        try {
            if ('' === $content = $response->getContent(false)) {
                Log::error('SnowflakeService: Response body is empty');
                throw new JsonException('Response body is empty.');
            }

            $headers = $response->getHeaders(false);
            Log::info('SnowflakeService: Response headers', [
                'headers' => array_keys($headers),
                'content_length' => strlen($content),
                'is_gzipped' => isset($headers['content-encoding'][0]) && $headers['content-encoding'][0] === 'gzip',
            ]);

            if ('gzip' === ($headers['content-encoding'][0] ?? null)) {
                Log::info('SnowflakeService: Decompressing gzipped content');
                $content = $this->gzdecode($content);
                Log::info('SnowflakeService: Content decompressed', [
                    'decompressed_length' => strlen($content),
                ]);
            }

            Log::info('SnowflakeService: Parsing JSON response');
            try {
                $content = json_decode($content, true, 512, JSON_BIGINT_AS_STRING | JSON_THROW_ON_ERROR);
                Log::info('SnowflakeService: JSON parsed successfully', [
                    'keys' => is_array($content) ? array_keys($content) : 'not-an-array',
                ]);
            } catch (JsonException $exception) {
                Log::error('SnowflakeService: JSON parse error', [
                    'error' => $exception->getMessage(),
                    'url' => $response->getInfo('url'),
                ]);
                throw new JsonException(sprintf('%s for "%s".', $exception->getMessage(), $response->getInfo('url')), $exception->getCode());
            }

            if (false === is_array($content)) {
                Log::error('SnowflakeService: Expected array but got different type', [
                    'type' => get_debug_type($content),
                    'url' => $response->getInfo('url'),
                ]);
                throw new JsonException(sprintf('JSON content was expected to decode to an array, "%s" returned for "%s".', get_debug_type($content), $response->getInfo('url')));
            }

            $statusCode = $response->getStatusCode();
            if ($statusCode >= 400) {
                Log::error('SnowflakeService: HTTP error response', [
                    'status_code' => $statusCode,
                    'message' => $content["message"] ?? 'no-message',
                ]);
                throw new Exception(sprintf('Snowflake error, %s returned with message: %s', $statusCode, $content["message"] ?? 'Unknown error'), $statusCode);
            }

            Log::info('SnowflakeService: Response converted to array successfully');
            return $content;
        } catch (Exception $e) {
            Log::error('SnowflakeService: Error converting response to array', [
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString(),
            ]);
            throw $e;
        }
    }

    private function gzdecode(string $data): string
    {
        Log::info('SnowflakeService: Decompressing gzipped data', [
            'data_length' => strlen($data),
        ]);

        try {
            $inflate = inflate_init(ZLIB_ENCODING_GZIP);
            Log::info('SnowflakeService: Initialized inflate');

            $content = '';
            $offset = 0;

            do {
                $chunk = inflate_add($inflate, substr($data, $offset));
                $content .= $chunk;

                Log::info('SnowflakeService: Processed chunk', [
                    'chunk_length' => strlen($chunk),
                    'processed_so_far' => strlen($content),
                ]);

                if (ZLIB_STREAM_END === inflate_get_status($inflate)) {
                    $offset += inflate_get_read_len($inflate);
                    Log::info('SnowflakeService: Reached stream end', [
                        'new_offset' => $offset,
                    ]);
                }
            } while ($offset < strlen($data));

            Log::info('SnowflakeService: Data decompressed successfully', [
                'decompressed_length' => strlen($content),
            ]);

            return $content;
        } catch (Exception $e) {
            Log::error('SnowflakeService: Error decompressing data', [
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString(),
            ]);
            throw $e;
        }
    }
}
