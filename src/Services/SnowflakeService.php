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
use LaravelSnowflakeApi\Traits\DebugLogging;

class SnowflakeService
{
    use DebugLogging;

    private const CODE_SUCCESS = '090001';
    private const CODE_ASYNC = '333334';

    /**
     * Initialize the Snowflake API service
     *
     * @param string $baseUrl The base URL for the Snowflake API
     * @param string $account The Snowflake account identifier
     * @param string $user The Snowflake username
     * @param string $publicKey The public key fingerprint
     * @param string $privateKey The private key content (PEM format)
     * @param string $privateKeyPassphrase The passphrase for the private key
     * @param string $warehouse The Snowflake warehouse to use
     * @param string $database The Snowflake database to use
     * @param string $schema The Snowflake schema to use
     * @param int $timeout Timeout in seconds for query execution
     */
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
        $this->debugLog('SnowflakeService: Initialized', [
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
        $this->debugLog('SnowflakeService: Testing connection');
        try {
            $token = $this->getAccessToken();
            $this->debugLog('SnowflakeService: Connection test successful - token generated');
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
        $this->debugLog('SnowflakeService: Starting query execution', [
            'query' => $query,
        ]);

        try {
            $statementId = $this->postStatement($query);
            $this->debugLog('SnowflakeService: Statement posted', ['statementId' => $statementId]);

            $result = $this->getResult($statementId);
            $this->debugLog('SnowflakeService: Initial result retrieved', [
                'executed' => $result->isExecuted(),
                'id' => $result->getId(),
            ]);

            $startTime = time();
            while (!$result->isExecuted()) {
                $timeElapsed = time() - $startTime;
                $this->debugLog('SnowflakeService: Waiting for execution to complete', [
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
                $this->debugLog('SnowflakeService: Checked result status', [
                    'executed' => $result->isExecuted(),
                ]);
            }

            $data = $result->getData();
            $this->debugLog('SnowflakeService: Initial data received', [
                'count' => count($data),
            ]);

            $pageNumber = 1;
            while ($result->getPaginationNext()) {
                $pageNumber++;
                $this->debugLog('SnowflakeService: Retrieving additional page', [
                    'page' => $pageNumber,
                ]);
                $newData = $result->getData();
                $this->debugLog('SnowflakeService: Additional data retrieved', [
                    'page' => $pageNumber,
                    'count' => count($newData),
                ]);
                $data = array_merge($data, $newData);
            }

            $collection = collect($data);
            $this->debugLog('SnowflakeService: Query execution completed', [
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

    /**
     * Generate a Snowflake API access token using JWT
     * 
     * This method will:
     * 1. Use the private key content provided in the constructor
     * 2. Create a JWT with the appropriate claims
     * 3. Sign it using RS256 algorithm
     * 4. Return the serialized token
     *
     * @return string The JWT access token for Snowflake API
     * @throws Exception If unable to generate the token
     */
    private function getAccessToken(): string
    {
        $this->debugLog('SnowflakeService: Generating access token');

        try {
            // Use the private key content directly
            $keyContent = $this->privateKey;
            
            if (empty($keyContent)) {
                throw new Exception("Private key content is empty");
            }
            
            // Replace literal '\n' sequences with actual newlines
            $keyContent = str_replace('\n', "\n", $keyContent);
            $this->debugLog('SnowflakeService: Prepared private key content', [
                'key_length' => strlen($keyContent),
                'contains_begin' => str_contains($keyContent, '-----BEGIN'),
                'contains_end' => str_contains($keyContent, '-----END'),
            ]);
            
            $this->debugLog('SnowflakeService: Creating JWK from private key');
            $privateKey = JWKFactory::createFromKey(
                $keyContent,
                $this->privateKeyPassphrase,
                [
                    'use' => 'sig',
                    'alg' => 'RS256',
                ]
            );

            // Log the complete JWK structure for debugging
            $this->debugLog('SnowflakeService: JWK details', [
                'jwk_keys' => array_keys($privateKey->all()),
                'jwk_values' => array_map(function($key) use ($privateKey) {
                    // Only log non-sensitive keys
                    return in_array($key, ['alg', 'use', 'kty']) ? $privateKey->get($key) : '[REDACTED]';
                }, array_keys($privateKey->all()))
            ]);
            
            $this->debugLog('SnowflakeService: JWK created successfully');

            $publicKeyFingerprint = 'SHA256:' . $this->publicKey;
            $this->debugLog('SnowflakeService: Using public key fingerprint', [
                'raw_public_key' => $this->publicKey,
                'fingerprint' => $publicKeyFingerprint,
            ]);
            
            $expires_in = time() + (60 * 60);
            $payload = [
                'iss' => sprintf('%s.%s', $this->user, $publicKeyFingerprint),
                'sub' => $this->user,
                'iat' => time(),
                'exp' => $expires_in,
            ];

            $this->debugLog('SnowflakeService: Creating JWT payload', [
                'iss' => $payload['iss'],
                'sub' => $payload['sub'],
                'iat' => $payload['iat'],
                'exp' => $payload['exp'],
                'exp_in_seconds' => $expires_in - time(),
                'publicKeyFingerprint' => $publicKeyFingerprint,
                'raw_payload' => json_encode($payload),
            ]);

            $algorithmManager = new AlgorithmManager([new RS256()]);
            $jwsBuilder = new JWSBuilder($algorithmManager);

            $this->debugLog('SnowflakeService: Building JWS');
            $jws = $jwsBuilder
                ->create()
                ->withPayload(json_encode($payload))
                ->addSignature($privateKey, ['alg' => 'RS256', 'typ' => 'JWT'])
                ->build();
            $this->debugLog('SnowflakeService: JWS built successfully');

            $serializer = new CompactSerializer();
            $access_token = $serializer->serialize($jws);

            // Log the first and last 10 characters of the token for debugging
            $token_start = substr($access_token, 0, 10);
            $token_end = substr($access_token, -10);
            $token_parts = explode('.', $access_token);
            
            $this->debugLog('SnowflakeService: Access token generated successfully', [
                'token_length' => strlen($access_token),
                'token_preview' => "{$token_start}...{$token_end}",
                'token_parts_count' => count($token_parts),
                'header_length' => strlen($token_parts[0] ?? ''),
                'payload_length' => strlen($token_parts[1] ?? ''),
                'signature_length' => strlen($token_parts[2] ?? ''),
            ]);

            // Debug the headers for verification
            if (isset($token_parts[0])) {
                $header = json_decode(base64_decode(str_replace(['-', '_'], ['+', '/'], $token_parts[0])), true);
                $this->debugLog('SnowflakeService: JWT Header', [
                    'alg' => $header['alg'] ?? 'unknown',
                    'typ' => $header['typ'] ?? 'unknown',
                ]);
            }

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
        $this->debugLog('SnowflakeService: Posting statement', [
            'statement' => $statement,
        ]);

        try {
            $variables = http_build_query([
                'async' => 'true',
                'nullable' => 'true'
            ]);

            $url = sprintf('https://%s.snowflakecomputing.com/api/v2/statements?%s', $this->account, $variables);
            $this->debugLog('SnowflakeService: Prepared URL for statement', [
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
            $this->debugLog('SnowflakeService: Prepared data for statement', [
                'warehouse' => $this->warehouse,
                'database' => $this->database,
                'schema' => $this->schema,
            ]);

            $this->debugLog('SnowflakeService: Getting headers for request');
            $headers = $this->getHeaders();
            $this->debugLog('SnowflakeService: Headers retrieved');

            $this->debugLog('SnowflakeService: Creating HTTP client');
            $httpClient = HttpClient::create();

            $this->debugLog('SnowflakeService: Sending POST request');
            $response = $httpClient->request('POST', $url, [
                'headers' => $headers,
                'json' => $data,
            ]);
            $this->debugLog('SnowflakeService: POST request sent');

            $this->debugLog('SnowflakeService: Converting response to array');
            $content = $this->toArray($response);
            $this->debugLog('SnowflakeService: Response converted to array');

            $this->debugLog('SnowflakeService: Validating result');
            $this->hasResult($content, [self::CODE_ASYNC]);
            $this->debugLog('SnowflakeService: Result validated');

            $this->debugLog('SnowflakeService: Statement posted successfully', [
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
        $this->debugLog('SnowflakeService: Getting statement results', [
            'statementId' => $id,
            'page' => $page,
        ]);

        try {
            $variables = http_build_query([
                'partition' => $page - 1,
            ]);

            $url = sprintf('https://%s.snowflakecomputing.com/api/v2/statements/%s?%s', $this->account, $id, $variables);
            $this->debugLog('SnowflakeService: Prepared URL for getting statement', [
                'url' => $url,
            ]);

            $this->debugLog('SnowflakeService: Getting headers for request');
            $headers = $this->getHeaders();

            $this->debugLog('SnowflakeService: Creating HTTP client');
            $httpClient = HttpClient::create();

            $this->debugLog('SnowflakeService: Sending GET request');
            $response = $httpClient->request('GET', $url, [
                'headers' => $headers,
            ]);
            $this->debugLog('SnowflakeService: GET request sent');

            $this->debugLog('SnowflakeService: Converting response to array');
            $result = $this->toArray($response);

            $this->debugLog('SnowflakeService: Statement results retrieved', [
                'code' => $result['code'] ?? 'no-code',
                'has_data' => isset($result['data']),
                'data_count' => isset($result['data']) ? count($result['data']) : 0,
            ]);

            return $result;
        } catch (Exception $e) {
            $this->debugLog('SnowflakeService: Error getting statement', [
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
        $this->debugLog('SnowflakeService: Cancelling statement', [
            'statementId' => $id,
        ]);

        try {
            $url = sprintf('https://%s.snowflakecomputing.com/api/v2/statements/%s/cancel', $this->account, $id);
            $this->debugLog('SnowflakeService: Prepared URL for cancelling statement', [
                'url' => $url,
            ]);

            $this->debugLog('SnowflakeService: Getting headers for request');
            $headers = $this->getHeaders();

            $this->debugLog('SnowflakeService: Creating HTTP client');
            $httpClient = HttpClient::create();

            $this->debugLog('SnowflakeService: Sending POST request for cancellation');
            $response = $httpClient->request('POST', $url, [
                'headers' => $headers,
            ]);
            $this->debugLog('SnowflakeService: POST request for cancellation sent');

            // Check the status code first
            $statusCode = $response->getStatusCode();
            
            // Consider any 2xx status code as success for cancellation
            if ($statusCode >= 200 && $statusCode < 300) {
                $this->debugLog('SnowflakeService: Cancellation successful based on status code', [
                    'statusCode' => $statusCode
                ]);
                
                // Try to get content, but don't fail if it's empty
                try {
                    $content = $response->getContent(false);
                    
                    if (!empty($content)) {
                        $this->debugLog('SnowflakeService: Converting response to array and validating');
                        $responseData = json_decode($content, true);
                        
                        if (is_array($responseData) && isset($responseData['code'])) {
                            $this->hasResult($responseData, [self::CODE_SUCCESS]);
                        } else {
                            $this->debugLog('SnowflakeService: Response not in expected format, but status code indicates success');
                        }
                    } else {
                        $this->debugLog('SnowflakeService: Empty response body but status code indicates success');
                    }
                } catch (Exception $contentException) {
                    // Log but don't throw - if status code was 2xx, we consider it a success
                    Log::warning('SnowflakeService: Could not process response body, but status code indicates success', [
                        'error' => $contentException->getMessage()
                    ]);
                }
            } else {
                Log::error('SnowflakeService: Cancellation failed with status code', [
                    'statusCode' => $statusCode
                ]);
                throw new Exception("Failed to cancel statement, received status code: {$statusCode}");
            }

            $this->debugLog('SnowflakeService: Statement cancelled successfully');
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
        $this->debugLog('SnowflakeService: Getting result for statement', [
            'statementId' => $id,
        ]);

        try {
            $this->debugLog('SnowflakeService: Fetching statement data for page 1');
            $data = $this->getStatement($id, 1);

            $executed = $data['code'] === self::CODE_SUCCESS;
            $this->debugLog('SnowflakeService: Statement execution status', [
                'executed' => $executed,
                'code' => $data['code'],
            ]);

            $this->debugLog('SnowflakeService: Creating Result object');
            $result = new Result($this);
            $result->setId($data['statementHandle']);
            $result->setExecuted($executed);

            if (false === $executed) {
                $this->debugLog('SnowflakeService: Statement not yet executed, returning partial result');
                return $result;
            }

            $this->debugLog('SnowflakeService: Validating response data structure');
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

            $this->debugLog('SnowflakeService: Populating Result object with data', [
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

            $this->debugLog('SnowflakeService: Result object created successfully');
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
        $this->debugLog('SnowflakeService: Validating result data', [
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

            $this->debugLog('SnowflakeService: Result validation successful');
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
        $this->debugLog('SnowflakeService: Generating request headers');

        try {
            $this->debugLog('SnowflakeService: Getting access token');
            $accessToken = $this->getAccessToken();
            
            // Log the full token in a safely printable format - ONLY FOR DEBUGGING
            // This should be removed in production
            $this->debugLog('SnowflakeService: Full JWT Token', [
                'token' => $accessToken
            ]);

            $headers = [
                sprintf('Authorization: Bearer %s', $accessToken),
                'Accept-Encoding: gzip',
                'User-Agent: SnowflakeService/0.5',
                'X-Snowflake-Authorization-Token-Type: KEYPAIR_JWT',
            ];

            $this->debugLog('SnowflakeService: Headers generated successfully', [
                'header_count' => count($headers),
                'authorization_prefix' => substr($headers[0], 0, 25) . '...',
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
        $this->debugLog('SnowflakeService: Converting HTTP response to array', [
            'status_code' => $response->getStatusCode(),
            'content_type' => $response->getHeaders(false)['content-type'][0] ?? 'unknown',
        ]);

        try {
            if ('' === $content = $response->getContent(false)) {
                Log::error('SnowflakeService: Response body is empty');
                throw new JsonException('Response body is empty.');
            }

            $headers = $response->getHeaders(false);
            $this->debugLog('SnowflakeService: Response headers', [
                'headers' => array_keys($headers),
                'content_length' => strlen($content),
                'is_gzipped' => isset($headers['content-encoding'][0]) && $headers['content-encoding'][0] === 'gzip',
            ]);

            if ('gzip' === ($headers['content-encoding'][0] ?? null)) {
                $this->debugLog('SnowflakeService: Decompressing gzipped content');
                $content = $this->gzdecode($content);
                $this->debugLog('SnowflakeService: Content decompressed', [
                    'decompressed_length' => strlen($content),
                ]);
            }

            $this->debugLog('SnowflakeService: Parsing JSON response');
            try {
                $content = json_decode($content, true, 512, JSON_BIGINT_AS_STRING | JSON_THROW_ON_ERROR);
                $this->debugLog('SnowflakeService: JSON parsed successfully', [
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
                    'error_details' => $content["data"] ?? [],
                    'full_response' => $content,
                ]);
                throw new Exception(sprintf('Snowflake error, %s returned with message: %s', $statusCode, $content["message"] ?? 'Unknown error'), $statusCode);
            }

            $this->debugLog('SnowflakeService: Response converted to array successfully');
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
        $this->debugLog('SnowflakeService: Decompressing gzipped data', [
            'data_length' => strlen($data),
        ]);

        try {
            $inflate = inflate_init(ZLIB_ENCODING_GZIP);
            $this->debugLog('SnowflakeService: Initialized inflate');

            $content = '';
            $offset = 0;

            do {
                $chunk = inflate_add($inflate, substr($data, $offset));
                $content .= $chunk;

                $this->debugLog('SnowflakeService: Processed chunk', [
                    'chunk_length' => strlen($chunk),
                    'processed_so_far' => strlen($content),
                ]);

                if (ZLIB_STREAM_END === inflate_get_status($inflate)) {
                    $offset += inflate_get_read_len($inflate);
                    $this->debugLog('SnowflakeService: Reached stream end', [
                        'new_offset' => $offset,
                    ]);
                }
            } while ($offset < strlen($data));

            $this->debugLog('SnowflakeService: Data decompressed successfully', [
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
