<?php

declare(strict_types=1);

namespace LaravelSnowflakeApi\Services;

use LaravelSnowflakeApi\Services\Result;
use LaravelSnowflakeApi\Services\SnowflakeConfig;
use LaravelSnowflakeApi\Exceptions\SnowflakeApiException;
use Exception;
use Illuminate\Support\Collection;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Cache;
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
     * Configuration for Snowflake API
     *
     * @var SnowflakeConfig
     */
    private $config;

    /**
     * HTTP Client instance for reuse
     * 
     * @var \Symfony\Contracts\HttpClient\HttpClientInterface
     */
    private $httpClient;

    /**
     * Cached access token to avoid regeneration on every request
     * 
     * @var string|null
     */
    private $cachedToken = null;

    /**
     * Expiry timestamp for the cached token
     * 
     * @var int
     */
    private $tokenExpiry = 0;

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
        string $baseUrl,
        string $account,
        string $user,
        string $publicKey,
        string $privateKey,
        string $privateKeyPassphrase,
        string $warehouse,
        string $database,
        string $schema,
        int $timeout
    ) {
        $this->config = new SnowflakeConfig(
            $baseUrl, 
            $account, 
            $user, 
            $publicKey, 
            $privateKey, 
            $privateKeyPassphrase, 
            $warehouse, 
            $database, 
            $schema, 
            $timeout
        );
        
        // Configure HTTP client with persistent connections
        $this->httpClient = HttpClient::create([
            'max_host_connections' => 5,
            'max_connections' => 10,
            'timeout' => $timeout,
            'http_version' => '2.0',
            'persistent_connections' => true
        ]);
        
        $this->debugLog('SnowflakeService: Initialized', [
            'baseUrl' => $this->config->getBaseUrl(),
            'account' => $this->config->getAccount(),
            'user' => $this->config->getUser(),
            'warehouse' => $this->config->getWarehouse(),
            'database' => $this->config->getDatabase(),
            'schema' => $this->config->getSchema(),
            'timeout' => $this->config->getTimeout(),
            'has_privateKey' => !empty($this->config->getPrivateKey()),
            'has_publicKey' => !empty($this->config->getPublicKey()),
            'has_passphrase' => !empty($this->config->getPrivateKeyPassphrase()),
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
            $this->handleError($e, 'Connection test failed');
            throw new SnowflakeApiException("Connection test failed: " . $e->getMessage(), 0, $e);
        }
    }

    /**
     * Execute a SQL query and return the results as a collection
     *
     * @param string $query The SQL query to execute
     * @return Collection The query results
     */
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
                    'timeout' => $this->config->getTimeout(),
                ]);

                if ($timeElapsed >= $this->config->getTimeout()) {
                    Log::warning('SnowflakeService: Query execution timed out', [
                        'statementId' => $statementId,
                        'timeout' => $this->config->getTimeout(),
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

            // Process result once all pages have been collected
            if ($result->getPageTotal() > 1) {
                $this->debugLog('SnowflakeService: Multiple pages detected, retrieving all pages');
                
                // Create requests without executing them yet
                $responses = [];
                for ($page = 2; $page <= $result->getPageTotal(); $page++) {
                    $this->debugLog('SnowflakeService: Preparing request for page ' . $page);
                    $url = sprintf(
                        'https://%s.snowflakecomputing.com/api/v2/statements/%s?%s', 
                        $this->config->getAccount(), 
                        $statementId, 
                        http_build_query(['partition' => $page - 1])
                    );
                    
                    $responses[$page] = $this->httpClient->request('GET', $url, [
                        'headers' => $this->getHeaders()
                    ]);
                }
                
                // Process the responses
                foreach ($responses as $page => $response) {
                    $pageData = $this->toArray($response);
                    $result->addPageData($pageData['data'] ?? []);
                    $this->debugLog('SnowflakeService: Retrieved page ' . $page);
                }
            }
            
            // Get the transformed data with column names as keys
            $data = $result->toArray();
            $this->debugLog('SnowflakeService: Processed result data', [
                'row_count' => count($data),
            ]);

            $collection = collect($data);
            $this->debugLog('SnowflakeService: Query execution completed', [
                'total_results' => $collection->count(),
            ]);

            return $collection;
        } catch (Exception $e) {
            $this->handleError($e, 'Error executing query', ['query' => $query]);
            throw $e;
        }
    }

    /**
     * Generate a Snowflake API access token using JWT
     * 
     * This method will:
     * 1. Return cached token if available and not expired
     * 2. Use the private key content provided in the constructor
     * 3. Create a JWT with the appropriate claims
     * 4. Sign it using RS256 algorithm
     * 5. Cache the token for future use
     * 6. Return the serialized token
     *
     * @return string The JWT access token for Snowflake API
     * @throws Exception If unable to generate the token
     */
    private function getAccessToken(): string
    {
        // Use static cache for token across instances
        static $staticTokenCache = null;
        static $staticTokenExpiry = 0;
        
        // Check static cache first for performance
        if ($staticTokenCache && time() < $staticTokenExpiry - 60) { // 1 minute buffer
            $this->debugLog('SnowflakeService: Using static cached access token');
            
            // Also set instance properties for backward compatibility
            $this->cachedToken = $staticTokenCache;
            $this->tokenExpiry = $staticTokenExpiry;
            
            return $staticTokenCache;
        }
        
        // Generate a cache key based on account and user
        $cacheKey = "snowflake_api_token:{$this->config->getAccount()}:{$this->config->getUser()}";
        
        // Try to get token from Laravel cache first
        $cachedTokenData = Cache::get($cacheKey);
        
        if ($cachedTokenData && 
            isset($cachedTokenData['token']) && 
            isset($cachedTokenData['expiry']) && 
            time() < $cachedTokenData['expiry'] - 60) { // 1 minute buffer
            
            $this->debugLog('SnowflakeService: Using cached access token from application cache');
            
            // Also set instance properties for backward compatibility
            $this->cachedToken = $cachedTokenData['token'];
            $this->tokenExpiry = $cachedTokenData['expiry'];
            
            // Update static cache
            $staticTokenCache = $cachedTokenData['token'];
            $staticTokenExpiry = $cachedTokenData['expiry'];
            
            return $cachedTokenData['token'];
        }

        $this->debugLog('SnowflakeService: Generating new access token');

        try {
            // Use the private key content directly
            $keyContent = $this->config->getPrivateKey();
            
            if (empty($keyContent)) {
                throw new Exception("Private key content is empty");
            }
            
            // Replace literal '\n' sequences with actual newlines
            $keyContent = str_replace('\n', "\n", $keyContent);
            $this->debugLog('SnowflakeService: Prepared private key content', [
                'key_length' => mb_strlen($keyContent, 'UTF-8'),
                'contains_begin' => mb_strpos($keyContent, '-----BEGIN') !== false,
                'contains_end' => mb_strpos($keyContent, '-----END') !== false,
            ]);
            
            $this->debugLog('SnowflakeService: Creating JWK from private key');
            $privateKey = JWKFactory::createFromKey(
                $keyContent,
                $this->config->getPrivateKeyPassphrase(),
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

            $publicKeyFingerprint = 'SHA256:' . $this->config->getPublicKey();
            $this->debugLog('SnowflakeService: Using public key fingerprint', [
                'raw_public_key' => $this->config->getPublicKey(),
                'fingerprint' => $publicKeyFingerprint,
            ]);
            
            $expires_in = time() + (60 * 60); // 1 hour expiry
            $payload = [
                'iss' => sprintf('%s.%s', $this->config->getUser(), $publicKeyFingerprint),
                'sub' => $this->config->getUser(),
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

            // Cache the token and its expiry in instance properties, Laravel cache, and static cache
            $this->cachedToken = $access_token;
            $this->tokenExpiry = $expires_in;
            
            // Update static cache
            $staticTokenCache = $access_token;
            $staticTokenExpiry = $expires_in;
            
            // Store token in Laravel cache with expiry
            $cacheData = [
                'token' => $access_token,
                'expiry' => $expires_in,
            ];
            
            // Cache until 1 minute before expiry
            $cacheDuration = $expires_in - time() - 60;
            Cache::put($cacheKey, $cacheData, $cacheDuration);
            
            $this->debugLog('SnowflakeService: Token stored in application cache', [
                'cache_key' => $cacheKey,
                'cache_duration' => $cacheDuration,
                'expiry_time' => date('Y-m-d H:i:s', $expires_in),
            ]);

            // Log the first and last 10 characters of the token for debugging
            $token_start = mb_substr($access_token, 0, 10, 'UTF-8');
            $token_end = mb_substr($access_token, -10, null, 'UTF-8');
            $token_parts = explode('.', $access_token);
            
            $this->debugLog('SnowflakeService: Access token generated successfully', [
                'token_length' => mb_strlen($access_token, 'UTF-8'),
                'token_preview' => "{$token_start}...{$token_end}",
                'token_parts_count' => count($token_parts),
                'header_length' => mb_strlen($token_parts[0] ?? '', 'UTF-8'),
                'payload_length' => mb_strlen($token_parts[1] ?? '', 'UTF-8'),
                'signature_length' => mb_strlen($token_parts[2] ?? '', 'UTF-8'),
                'token_cached' => true,
                'token_expires' => date('Y-m-d H:i:s', $this->tokenExpiry),
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
            $this->handleError($e, 'Error generating access token');
            throw new SnowflakeApiException("Failed to generate access token: " . $e->getMessage(), 0, $e);
        }
    }

    /**
     * Post a SQL statement to Snowflake for execution
     *
     * @param string $statement The SQL statement to execute
     * @return string The statement handle ID
     */
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

            $url = sprintf('https://%s.snowflakecomputing.com/api/v2/statements?%s', $this->config->getAccount(), $variables);
            
            $data = [
                'statement' => $statement,
                'warehouse' => $this->config->getWarehouse(),
                'database' => $this->config->getDatabase(),
                'schema' => $this->config->getSchema(),
                'resultSetMetaData' => [
                    'format' => 'json',
                ],
                'parameters' => [
                    'DATE_OUTPUT_FORMAT' => 'YYYY-MM-DD',
                    'TIME_OUTPUT_FORMAT' => 'HH24:MI:SS.FF',
                    'TIMESTAMP_OUTPUT_FORMAT' => 'YYYY-MM-DD HH24:MI:SS.FF',
                    'TIMESTAMP_NTZ_OUTPUT_FORMAT' => 'YYYY-MM-DD HH24:MI:SS.FF',
                    'TIMESTAMP_LTZ_OUTPUT_FORMAT' => 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM',
                    'TIMESTAMP_TZ_OUTPUT_FORMAT' => 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM'
                ]
            ];
            
            $content = $this->makeRequest('POST', $url, [
                'json' => $data,
            ]);

            $this->hasResult($content, [self::CODE_ASYNC]);

            $this->debugLog('SnowflakeService: Statement posted successfully', [
                'statementHandle' => $content['statementHandle'],
            ]);

            return $content['statementHandle'];
        } catch (Exception $e) {
            $this->handleError($e, 'Error posting statement', ['statement' => $statement]);
            throw $e;
        }
    }

    /**
     * Get statement results for a specific page
     *
     * @param string $id Statement handle ID
     * @param int $page Page number to retrieve (1-based)
     * @return array The statement results
     */
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

            $url = sprintf('https://%s.snowflakecomputing.com/api/v2/statements/%s?%s', 
                $this->config->getAccount(), 
                $id, 
                $variables
            );
            
            $result = $this->makeRequest('GET', $url);

            $this->debugLog('SnowflakeService: Statement results retrieved', [
                'code' => $result['code'] ?? 'no-code',
                'has_data' => isset($result['data']),
                'data_count' => isset($result['data']) ? count($result['data']) : 0,
            ]);

            return $result;
        } catch (Exception $e) {
            $this->handleError($e, 'Error getting statement', [
                'statementId' => $id,
                'page' => $page,
            ]);
            throw $e;
        }
    }

    /**
     * Cancel a running statement
     *
     * @param string $id Statement handle ID to cancel
     */
    public function cancelStatement(string $id): void
    {
        $this->debugLog('SnowflakeService: Cancelling statement', [
            'statementId' => $id,
        ]);

        try {
            $url = sprintf('https://%s.snowflakecomputing.com/api/v2/statements/%s/cancel', 
                $this->config->getAccount(), 
                $id
            );
            
            $response = $this->httpClient->request('POST', $url, [
                'headers' => $this->getHeaders(),
            ]);
            
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
                        $responseData = json_decode($content, true);
                        
                        if (is_array($responseData) && isset($responseData['code'])) {
                            $this->hasResult($responseData, [self::CODE_SUCCESS]);
                        }
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
            $this->handleError($e, 'Error cancelling statement', ['statementId' => $id]);
            throw $e;
        }
    }

    /**
     * Get the result for a statement
     *
     * @param string $id Statement handle ID
     * @return Result The statement result
     */
    public function getResult(string $id): Result
    {
        $this->debugLog('SnowflakeService: Getting result for statement', [
            'statementId' => $id,
        ]);

        try {
            $data = $this->getStatement($id, 1);
            return $this->processResultData($data);
        } catch (Exception $e) {
            $this->handleError($e, 'Error getting result', ['statementId' => $id]);
            throw $e;
        }
    }

    /**
     * Process and validate result data into a Result object
     *
     * @param array $data The data to process
     * @return Result The processed result
     */
    private function processResultData(array $data): Result
    {
        $executed = $data['code'] === self::CODE_SUCCESS;
        
        $result = new Result($this);
        $result->setId($data['statementHandle']);
        $result->setExecuted($executed);

        if (false === $executed) {
            return $result;
        }

        $this->validateResultStructure($data);

        $result->setTotal($data['resultSetMetaData']['numRows']);
        $result->setPage(1);
        $result->setPageTotal(count($data['resultSetMetaData']['partitionInfo']));
        $result->setFields($data['resultSetMetaData']['rowType']);
        $result->setData($data['data']);
        $result->setTimestamp($data['createdOn']);

        return $result;
    }

    /**
     * Validate the structure of result data
     *
     * @param array $data The data to validate
     * @throws Exception If validation fails
     */
    private function validateResultStructure(array $data): void
    {
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
    }

    /**
     * Validate that a result has expected codes
     *
     * @param array $data The data to validate
     * @param array $codes Expected valid codes
     * @throws Exception If validation fails
     */
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
        } catch (Exception $e) {
            $this->handleError($e, 'Result validation failed');
            throw $e;
        }
    }

    /**
     * Get headers for API requests
     *
     * @return array Headers
     */
    private function getHeaders(): array
    {
        $this->debugLog('SnowflakeService: Generating request headers');

        try {
            $accessToken = $this->getAccessToken();
            
            return [
                sprintf('Authorization: Bearer %s', $accessToken),
                'Accept-Encoding: gzip',
                'User-Agent: SnowflakeService/0.5',
                'X-Snowflake-Authorization-Token-Type: KEYPAIR_JWT',
            ];
        } catch (Exception $e) {
            $this->handleError($e, 'Error generating headers');
            throw $e;
        }
    }

    /**
     * Make a request to the Snowflake API
     *
     * @param string $method HTTP method (GET, POST, etc.)
     * @param string $url URL to request
     * @param array $options Request options
     * @return array Response as array
     */
    private function makeRequest(string $method, string $url, array $options = []): array
    {
        $this->debugLog('SnowflakeService: Making request', [
            'method' => $method,
            'url' => $url,
        ]);

        try {
            $options['headers'] = $this->getHeaders();
            
            $response = $this->httpClient->request($method, $url, $options);
            
            return $this->toArray($response);
        } catch (Exception $e) {
            $this->handleError($e, 'Error making request', [
                'method' => $method,
                'url' => $url,
            ]);
            throw $e;
        }
    }

    /**
     * Convert a ResponseInterface to an array
     *
     * @param ResponseInterface $response The response to convert
     * @return array The response as array
     */
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

            // Log raw response for debugging
            Log::debug('SnowflakeService: Raw response content', [
                'content_length' => mb_strlen($content, 'UTF-8'),
                'content_preview' => mb_substr($content, 0, 1000, 'UTF-8'),
                'url' => $response->getInfo('url')
            ]);

            $headers = $response->getHeaders(false);

            // Check if content is gzipped
            if ('gzip' === ($headers['content-encoding'][0] ?? null)) {
                $this->debugLog('SnowflakeService: Detected gzipped content, attempting decompression');
                try {
                    $content = $this->gzdecode($content);
                    $this->debugLog('SnowflakeService: Successfully decompressed content', [
                        'decompressed_length' => mb_strlen($content, 'UTF-8'),
                        'decompressed_preview' => mb_substr($content, 0, 1000, 'UTF-8')
                    ]);
                } catch (Exception $e) {
                    Log::error('SnowflakeService: Failed to decompress gzipped content', [
                        'error' => $e->getMessage(),
                        'content_length' => mb_strlen($content, 'UTF-8'),
                        'content_preview' => mb_substr($content, 0, 1000, 'UTF-8')
                    ]);
                    throw new JsonException('Failed to decompress gzipped response: ' . $e->getMessage());
                }
            }

            // Remove any invisible control characters using mb_ereg_replace
            $content = mb_ereg_replace('[\x00-\x1F\x7F]', '', $content, 'm');

            try {
                $content = json_decode($content, true, 512, JSON_BIGINT_AS_STRING | JSON_THROW_ON_ERROR);
            } catch (JsonException $exception) {
                // Log detailed error information
                Log::error('SnowflakeService: JSON decode error', [
                    'error' => $exception->getMessage(),
                    'url' => $response->getInfo('url'),
                    'content_length' => mb_strlen($content, 'UTF-8'),
                    'content_preview' => mb_substr($content, 0, 1000, 'UTF-8'),
                    'content_type' => $headers['content-type'][0] ?? 'unknown',
                    'content_encoding' => $headers['content-encoding'][0] ?? 'none'
                ]);
                
                throw new JsonException(sprintf('%s for "%s".', $exception->getMessage(), $response->getInfo('url')), $exception->getCode());
            }

            if (false === is_array($content)) {
                throw new JsonException(sprintf('JSON content was expected to decode to an array, "%s" returned for "%s".', get_debug_type($content), $response->getInfo('url')));
            }

            $statusCode = $response->getStatusCode();
            if ($statusCode >= 400) {
                throw new Exception(sprintf('Snowflake error, %s returned with message: %s', $statusCode, $content["message"] ?? 'Unknown error'), $statusCode);
            }

            return $content;
        } catch (Exception $e) {
            $this->handleError($e, 'Error converting response to array');
            throw $e;
        }
    }

    /**
     * Decompress gzipped content
     *
     * @param string $data The compressed data
     * @return string The decompressed content
     */
    private function gzdecode(string $data): string
    {
        $this->debugLog('SnowflakeService: Decompressing gzipped data', [
            'data_length' => mb_strlen($data, 'UTF-8'),
        ]);

        try {
            // First try using gzdecode
            $result = gzdecode($data);
            if ($result !== false) {
                $this->debugLog('SnowflakeService: Successfully decompressed using gzdecode');
                return $result;
            }

            // If gzdecode fails, try using inflate
            $inflate = inflate_init(ZLIB_ENCODING_GZIP);
            if ($inflate === false) {
                throw new Exception('Failed to initialize inflate');
            }
            
            $content = '';
            $offset = 0;

            do {
                $chunk = inflate_add($inflate, mb_substr($data, $offset, null, 'UTF-8'));
                if ($chunk === false) {
                    throw new Exception('Failed to decompress chunk at offset ' . $offset);
                }
                $content .= $chunk;

                if (ZLIB_STREAM_END === inflate_get_status($inflate)) {
                    $offset += inflate_get_read_len($inflate);
                }
            } while ($offset < mb_strlen($data, 'UTF-8'));

            $this->debugLog('SnowflakeService: Successfully decompressed using inflate');
            return $content;
        } catch (Exception $e) {
            $this->handleError($e, 'Error decompressing data');
            throw $e;
        }
    }

    /**
     * Handle and log errors consistently
     *
     * @param Exception $e The exception to handle
     * @param string $context Context information
     * @param array $additionalData Additional data to log
     */
    private function handleError(Exception $e, string $context, array $additionalData = []): void
    {
        $errorData = array_merge([
            'error' => $e->getMessage(),
            'trace' => $e->getTraceAsString(),
            'context' => $context
        ], $additionalData);
        
        Log::error('SnowflakeService: ' . $context, $errorData);
    }
}
