<?php

declare(strict_types=1);

namespace LaravelSnowflakeApi\Services;

use Exception;
use Illuminate\Support\Collection;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\Log;
use Jose\Component\Core\AlgorithmManager;
use Jose\Component\KeyManagement\JWKFactory;
use Jose\Component\Signature\Algorithm\RS256;
use Jose\Component\Signature\JWSBuilder;
use Jose\Component\Signature\Serializer\CompactSerializer;
use LaravelSnowflakeApi\Exceptions\SnowflakeApiException;
use LaravelSnowflakeApi\Traits\DebugLogging;
use Symfony\Component\HttpClient\Exception\JsonException;
use Symfony\Component\HttpClient\HttpClient;
use Symfony\Contracts\HttpClient\ResponseInterface;

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
     * Timestamp when HTTP client was created
     *
     * @var int
     */
    private $httpClientCreatedAt = 0;

    /**
     * Maximum age in seconds before recreating HTTP client
     * Recreating the client prevents connection leaks, DNS cache staleness,
     * and HTTP/2 connection age issues in long-running processes (Octane/FrankenPHP)
     *
     * @var int
     */
    private const HTTP_CLIENT_MAX_AGE = 3600; // 1 hour

    /**
     * Thread-safe token provider
     *
     * @var ThreadSafeTokenProvider
     */
    private $tokenProvider;

    /**
     * Initialize the Snowflake API service
     *
     * @param  string  $baseUrl  The base URL for the Snowflake API
     * @param  string  $account  The Snowflake account identifier
     * @param  string  $user  The Snowflake username
     * @param  string  $publicKey  The public key fingerprint
     * @param  string  $privateKey  The private key content (PEM format)
     * @param  string  $privateKeyPassphrase  The passphrase for the private key
     * @param  string  $warehouse  The Snowflake warehouse to use
     * @param  string  $database  The Snowflake database to use
     * @param  string  $schema  The Snowflake schema to use
     * @param  int  $timeout  Timeout in seconds for query execution
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

        // Initialize thread-safe token provider
        $this->tokenProvider = new ThreadSafeTokenProvider($this->config);

        // HTTP client will be created lazily on first use via getHttpClient()
        // This allows periodic recreation in long-running processes
        $this->httpClient = null;
        $this->httpClientCreatedAt = 0;

        $this->debugLog('SnowflakeService: Initialized', [
            'baseUrl' => $this->config->getBaseUrl(),
            'account' => $this->config->getAccount(),
            'user' => $this->config->getUser(),
            'warehouse' => $this->config->getWarehouse(),
            'database' => $this->config->getDatabase(),
            'schema' => $this->config->getSchema(),
            'timeout' => $this->config->getTimeout(),
            'has_privateKey' => ! empty($this->config->getPrivateKey()),
            'has_publicKey' => ! empty($this->config->getPublicKey()),
            'has_passphrase' => ! empty($this->config->getPrivateKeyPassphrase()),
        ]);
    }

    /**
     * Get or create HTTP client with automatic recreation for long-running processes
     *
     * This method implements periodic client recreation to address three issues
     * in long-running PHP processes (Laravel Octane, FrankenPHP):
     *
     * 1. **TCP Connection Leaks**: Symfony HttpClient keeps connections ESTABLISHED
     *    indefinitely. Without recreation, a 24-hour worker can accumulate 240+
     *    connections, leading to file descriptor exhaustion.
     *
     * 2. **DNS Cache Staleness**: DNS resolutions are cached for the client lifetime.
     *    If Snowflake's IPs change (load balancing, failover), the cached DNS
     *    causes connection failures.
     *
     * 3. **HTTP/2 Connection Age**: Snowflake may close idle HTTP/2 connections
     *    server-side after 2-3 hours, but cURL keeps trying to reuse them,
     *    causing "connection reset by peer" errors.
     *
     * **Performance Impact**: Recreation overhead is ~1-5ms, amortized over ~3600 requests
     * per hour = 0.0000004% overhead per request (negligible).
     *
     * **Connection Reuse**: Within the 1-hour window, HTTP keep-alive and HTTP/2
     * multiplexing work normally, providing full performance benefits.
     *
     * @return \Symfony\Contracts\HttpClient\HttpClientInterface
     */
    private function getHttpClient()
    {
        $now = time();

        // Recreate client if null or older than max age
        if ($this->httpClient === null || ($now - $this->httpClientCreatedAt) > self::HTTP_CLIENT_MAX_AGE) {
            $clientOptions = [
                'timeout' => $this->config->getTimeout(),
                'http_version' => '2.0',
                'max_redirects' => 5,
                'verify_peer' => true,
                'verify_host' => true,
            ];

            // Add connection lifetime limit if PHP 8.2+ and cURL 7.80+
            // This forces cURL to close and reopen connections older than 30 minutes
            // providing defense-in-depth against connection age issues
            if (defined('CURLOPT_MAXLIFETIME_CONN')) {
                $clientOptions['extra'] = [
                    'curl' => [
                        CURLOPT_MAXLIFETIME_CONN => 1800, // 30 minutes
                    ],
                ];
            }

            $this->httpClient = HttpClient::create($clientOptions);
            $this->httpClientCreatedAt = $now;

            $this->debugLog('SnowflakeService: HTTP client recreated', [
                'timestamp' => date('Y-m-d H:i:s', $now),
                'next_recreation_at' => date('Y-m-d H:i:s', $now + self::HTTP_CLIENT_MAX_AGE),
                'has_maxlifetime_conn' => defined('CURLOPT_MAXLIFETIME_CONN'),
            ]);
        }

        return $this->httpClient;
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
            throw new SnowflakeApiException('Connection test failed: '.$e->getMessage(), 0, $e);
        }
    }

    /**
     * Execute a SQL query and return the results as a collection
     *
     * @param  string  $query  The SQL query to execute
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
            while (! $result->isExecuted()) {
                $timeElapsed = time() - $startTime;

                if ($timeElapsed >= $this->config->getTimeout()) {
                    $this->cancelStatement($statementId);

                    return collect();
                }

                // Reduce blocking time
                usleep(250000); // 0.25 seconds instead of 1
                $result = $this->getResult($statementId);
            }

            // Process result once all pages have been collected
            if ($result->getPageTotal() > 1) {
                $this->debugLog('SnowflakeService: Multiple pages detected, retrieving all pages');

                // Create and process requests concurrently
                $responses = [];
                foreach (range(2, $result->getPageTotal()) as $page) {
                    $url = sprintf(
                        'https://%s.snowflakecomputing.com/api/v2/statements/%s?%s',
                        $this->config->getAccount(),
                        $statementId,
                        http_build_query(['partition' => $page - 1])
                    );
                    $responses[$page] = $this->getHttpClient()->request('GET', $url, [
                        'headers' => $this->getHeaders(),
                    ]);
                }

                // Process responses as they complete
                foreach ($this->getHttpClient()->stream($responses) as $response => $chunk) {
                    if ($chunk->isFirst()) {
                        $this->debugLog('SnowflakeService: Started receiving page response');
                    } elseif ($chunk->isLast()) {
                        $pageData = $this->toArray($response);
                        $result->addPageData($pageData['data'] ?? []);
                    }
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
     * Get a valid Snowflake API access token
     *
     * This method delegates to ThreadSafeTokenProvider which implements:
     * - Atomic token generation (prevents thundering herd)
     * - Double-checked locking (optimizes performance)
     * - Hierarchical caching (static -> Laravel cache -> generate)
     * - Graceful degradation (falls back if locking fails)
     *
     * @return string The JWT access token for Snowflake API
     *
     * @throws SnowflakeApiException If unable to generate the token
     */
    private function getAccessToken(): string
    {
        return $this->tokenProvider->getToken();
    }

    /**
     * Post a SQL statement to Snowflake for execution
     *
     * @param  string  $statement  The SQL statement to execute
     * @return string The statement handle ID
     */
    public function postStatement(string $statement): string
    {
        $this->debugLog('SnowflakeService: Posting statement', [
            'statement' => $statement,
        ]);

        try {
            $url = $this->buildApiUrl('statements', [
                'async' => 'true',
                'nullable' => 'true',
            ]);

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
                    'TIMESTAMP_TZ_OUTPUT_FORMAT' => 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM',
                ],
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
     * @param  string  $id  Statement handle ID
     * @param  int  $page  Page number to retrieve (1-based)
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
     * @param  string  $id  Statement handle ID to cancel
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

            $response = $this->getHttpClient()->request('POST', $url, [
                'headers' => $this->getHeaders(),
            ]);

            // Check the status code first
            $statusCode = $response->getStatusCode();

            // Consider any 2xx status code as success for cancellation
            if ($statusCode >= 200 && $statusCode < 300) {
                $this->debugLog('SnowflakeService: Cancellation successful based on status code', [
                    'statusCode' => $statusCode,
                ]);

                // Try to get content, but don't fail if it's empty
                try {
                    $content = $response->getContent(false);

                    if (! empty($content)) {
                        $responseData = json_decode($content, true);

                        if (is_array($responseData) && isset($responseData['code'])) {
                            $this->hasResult($responseData, [self::CODE_SUCCESS]);
                        }
                    }
                } catch (Exception $contentException) {
                    // Log but don't throw - if status code was 2xx, we consider it a success
                    Log::warning('SnowflakeService: Could not process response body, but status code indicates success', [
                        'error' => $contentException->getMessage(),
                    ]);
                }
            } else {
                Log::error('SnowflakeService: Cancellation failed with status code', [
                    'statusCode' => $statusCode,
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
     * @param  string  $id  Statement handle ID
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
     * @param  array  $data  The data to process
     * @return Result The processed result
     */
    private function processResultData(array $data): Result
    {
        $executed = $data['code'] === self::CODE_SUCCESS;

        $result = new Result($this);
        $result->setId($data['statementHandle']);
        $result->setExecuted($executed);

        if ($executed === false) {
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
     * @param  array  $data  The data to validate
     *
     * @throws Exception If validation fails
     */
    private function validateResultStructure(array $data): void
    {
        // Check for all required fields at once
        $requiredTopLevelFields = ['resultSetMetaData', 'data', 'createdOn'];
        $missingTopLevelFields = array_diff($requiredTopLevelFields, array_keys($data));

        if (! empty($missingTopLevelFields)) {
            $errorMsg = sprintf('Objects "%s" not found', implode(', ', $missingTopLevelFields));
            Log::error('SnowflakeService: Required fields missing in response', [
                'missing_fields' => $missingTopLevelFields,
                'keys_present' => array_keys($data),
            ]);
            throw new Exception($errorMsg);
        }

        // Only proceed if metadata exists
        $requiredMetaFields = ['numRows', 'partitionInfo', 'rowType'];
        $missingMetaFields = array_diff($requiredMetaFields, array_keys($data['resultSetMetaData']));

        if (! empty($missingMetaFields)) {
            $errorMsg = sprintf('Objects "%s" in "resultSetMetaData" not found', implode(', ', $missingMetaFields));
            Log::error('SnowflakeService: Required metadata fields missing in response', [
                'missing_fields' => $missingMetaFields,
                'keys_present' => array_keys($data['resultSetMetaData']),
            ]);
            throw new Exception($errorMsg);
        }
    }

    /**
     * Validate that a result has expected codes
     *
     * @param  array  $data  The data to validate
     * @param  array  $codes  Expected valid codes
     *
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
                if (array_key_exists($field, $data) === false) {
                    Log::error('SnowflakeService: Required field missing in response', [
                        'field' => $field,
                        'keys_present' => array_keys($data),
                    ]);
                    throw new Exception('Unacceptable result', 406);
                }
            }

            if (in_array($data['code'], $codes) === false) {
                Log::error('SnowflakeService: Unexpected result code', [
                    'expected_codes' => $codes,
                    'actual_code' => $data['code'],
                    'message' => $data['message'],
                ]);
                throw new Exception(sprintf('%s (%s)', $data['message'], $data['code']), 422);
            }

            foreach (['statementHandle', 'statementStatusUrl'] as $field) {
                if (array_key_exists($field, $data) === false) {
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
     * Token retrieval is handled by ThreadSafeTokenProvider which
     * implements its own caching strategy
     *
     * @return array HTTP headers for Snowflake API requests
     */
    private function getHeaders(): array
    {
        $accessToken = $this->getAccessToken();

        return [
            sprintf('Authorization: Bearer %s', $accessToken),
            'Accept-Encoding: gzip',
            'User-Agent: SnowflakeService/0.5',
            'X-Snowflake-Authorization-Token-Type: KEYPAIR_JWT',
        ];
    }

    /**
     * Make a request to the Snowflake API
     *
     * @param  string  $method  HTTP method (GET, POST, etc.)
     * @param  string  $url  URL to request
     * @param  array  $options  Request options
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

            $response = $this->getHttpClient()->request($method, $url, $options);

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
     * @param  ResponseInterface  $response  The response to convert
     * @return array The response as array
     */
    private function toArray(ResponseInterface $response): array
    {
        try {
            $content = $response->getContent(false);
            if (empty($content)) {
                throw new JsonException('Response body is empty.');
            }

            $headers = $response->getHeaders(false);

            // Handle gzip content in one step
            if ('gzip' === ($headers['content-encoding'][0] ?? null)) {
                $content = $this->gzdecode($content);
            }

            // More efficient than using mb_ereg_replace for all characters
            // Only clean if necessary - detect control chars first
            if (preg_match('/[\x00-\x1F\x7F]/', $content)) {
                $content = preg_replace('/[\x00-\x1F\x7F]/', '', $content);
            }

            // Decode with optimized options
            $data = json_decode($content, true, 512, JSON_BIGINT_AS_STRING | JSON_THROW_ON_ERROR);

            $statusCode = $response->getStatusCode();
            if ($statusCode >= 400) {
                throw new Exception(sprintf(
                    'Snowflake error, %s returned with message: %s',
                    $statusCode,
                    $data['message'] ?? 'Unknown error'
                ), $statusCode);
            }

            return $data;
        } catch (Exception $e) {
            $this->handleError($e, 'Error converting response to array');
            throw $e;
        }
    }

    /**
     * Decompress gzipped content
     *
     * @param  string  $data  The compressed data
     * @return string The decompressed content
     */
    private function gzdecode(string $data): string
    {
        // First try native function which is more efficient
        $result = @gzdecode($data);
        if ($result !== false) {
            return $result;
        }

        // Only use fallback when necessary
        $inflate = inflate_init(ZLIB_ENCODING_GZIP);
        if ($inflate === false) {
            throw new Exception('Failed to initialize inflate');
        }

        // Process larger chunks to reduce iterations
        $content = '';
        $chunkSize = 8192; // 8KB chunks instead of byte-by-byte
        $offset = 0;
        $dataLength = mb_strlen($data, 'UTF-8');

        do {
            $chunk = inflate_add(
                $inflate,
                mb_substr($data, $offset, min($chunkSize, $dataLength - $offset), 'UTF-8')
            );
            if ($chunk === false) {
                throw new Exception('Failed to decompress chunk at offset '.$offset);
            }
            $content .= $chunk;

            if (inflate_get_status($inflate) === ZLIB_STREAM_END) {
                $offset += inflate_get_read_len($inflate);
            }
        } while ($offset < $dataLength);

        return $content;
    }

    /**
     * Handle and log errors consistently
     *
     * @param  Exception  $e  The exception to handle
     * @param  string  $context  Context information
     * @param  array  $additionalData  Additional data to log
     */
    private function handleError(Exception $e, string $context, array $additionalData = []): void
    {
        $errorData = array_merge([
            'error' => $e->getMessage(),
            'trace' => $e->getTraceAsString(),
            'context' => $context,
        ], $additionalData);

        Log::error('SnowflakeService: '.$context, $errorData);
    }

    /**
     * Generate a Snowflake API endpoint URL
     */
    private function buildApiUrl(string $endpoint, array $params = []): string
    {
        $baseUrl = sprintf('https://%s.snowflakecomputing.com/api/v2', $this->config->getAccount());
        $fullUrl = $baseUrl.'/'.ltrim($endpoint, '/');

        if (! empty($params)) {
            $fullUrl .= '?'.http_build_query($params);
        }

        return $fullUrl;
    }
}
