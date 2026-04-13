<?php

declare(strict_types=1);

namespace LaravelSnowflakeApi\Services;

use Exception;
use Illuminate\Support\Collection;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\Log;
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
     * Whether diagnostic logging is enabled
     * Set via SNOWFLAKE_DIAGNOSTIC_LOGGING=true
     *
     * @var bool|null
     */
    private $diagnosticLoggingEnabled = null;

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
     * @param  string|null  $cacheDriver  The Laravel cache store to use for Snowflake tokens
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
        int $timeout,
        ?string $cacheDriver = null
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
            $timeout,
            $cacheDriver
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
            'cache_driver' => $this->config->getCacheDriver(),
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
                $this->debugLog('SnowflakeService: Multiple pages detected, retrieving all pages', [
                    'total_pages' => $result->getPageTotal(),
                ]);

                // Create and process requests concurrently
                $responses = [];
                $pageToResponseMap = [];
                foreach (range(2, $result->getPageTotal()) as $page) {
                    $url = sprintf(
                        'https://%s.snowflakecomputing.com/api/v2/statements/%s?%s',
                        $this->config->getAccount(),
                        $statementId,
                        http_build_query(['partition' => $page - 1])
                    );
                    $response = $this->getHttpClient()->request('GET', $url, [
                        'headers' => $this->getHeaders(),
                    ]);
                    $responses[$page] = $response;
                    $pageToResponseMap[spl_object_id($response)] = $page;
                }

                // Process responses as they complete, with retry for failures
                $failedPages = [];
                foreach ($this->getHttpClient()->stream($responses) as $response => $chunk) {
                    if ($chunk->isFirst()) {
                        $this->debugLog('SnowflakeService: Started receiving page response');
                    } elseif ($chunk->isLast()) {
                        $pageNumber = $pageToResponseMap[spl_object_id($response)] ?? 'unknown';
                        try {
                            $pageData = $this->toArray($response);
                            $result->addPageData($pageData['data'] ?? []);
                        } catch (Exception $e) {
                            // Capture detailed response info for diagnostics
                            if ($this->isDiagnosticLoggingEnabled()) {
                                $streamingDetails = [
                                    'context' => 'streaming_partition_failure',
                                    'statement_id' => $statementId,
                                    'partition' => $pageNumber,
                                    'total_partitions' => $result->getPageTotal(),
                                    'error_message' => $e->getMessage(),
                                    'error_code' => $e->getCode(),
                                    'chunk_is_timeout' => $chunk->isTimeout(),
                                ];

                                // Try to get response info even though it may have failed
                                try {
                                    $info = $response->getInfo();
                                    $streamingDetails['http_code'] = $info['http_code'] ?? null;
                                    $streamingDetails['total_time_ms'] = isset($info['total_time']) ? round($info['total_time'] * 1000, 2) : null;
                                    $streamingDetails['primary_ip'] = $info['primary_ip'] ?? 'unknown';
                                    $streamingDetails['url'] = $info['url'] ?? 'unknown';
                                } catch (Exception $infoError) {
                                    $streamingDetails['info_error'] = $infoError->getMessage();
                                }

                                $this->diagnosticLog('Partition fetch failed during streaming', $streamingDetails);
                            }

                            // Track failed pages for retry
                            $this->debugLog('SnowflakeService: Page fetch failed, will retry', [
                                'page' => $pageNumber,
                                'error' => $e->getMessage(),
                            ]);
                            $failedPages[] = $pageNumber;
                        }
                    }
                }

                // Retry failed pages with exponential backoff
                if (! empty($failedPages)) {
                    $this->debugLog('SnowflakeService: Retrying failed pages', [
                        'failed_pages' => $failedPages,
                    ]);

                    foreach ($failedPages as $page) {
                        $retryData = $this->fetchPartitionWithRetry($statementId, $page);
                        if ($retryData !== null) {
                            $result->addPageData($retryData);
                        }
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
     * Fetch a partition with retry logic and exponential backoff
     *
     * This method handles transient failures (HTTP 5xx) when fetching result
     * partitions from Snowflake. Snowflake's API occasionally returns 500 errors
     * on large result sets, particularly for higher partition numbers.
     *
     * Retry strategy:
     * - Up to 3 attempts (configurable via MAX_PARTITION_RETRIES)
     * - Exponential backoff: 500ms, 1000ms, 2000ms
     * - Only retries on 5xx errors (server-side transient failures)
     * - Logs each retry attempt for debugging
     *
     * @param  string  $statementId  Statement handle ID
     * @param  int  $page  Page number to retrieve (1-based)
     * @param  int  $maxRetries  Maximum retry attempts (default: 3)
     * @return array|null The partition data, or null if all retries fail
     */
    private function fetchPartitionWithRetry(string $statementId, int $page, int $maxRetries = 3): ?array
    {
        $baseDelayMs = 500;
        $retryStartTime = microtime(true);

        for ($attempt = 1; $attempt <= $maxRetries; $attempt++) {
            $attemptStartTime = microtime(true);

            try {
                $this->debugLog('SnowflakeService: Fetching partition with retry', [
                    'statementId' => $statementId,
                    'page' => $page,
                    'attempt' => $attempt,
                    'max_retries' => $maxRetries,
                ]);

                $result = $this->getStatement($statementId, $page);

                $attemptDuration = round((microtime(true) - $attemptStartTime) * 1000, 2);
                $this->debugLog('SnowflakeService: Partition retry successful', [
                    'page' => $page,
                    'attempt' => $attempt,
                    'data_count' => isset($result['data']) ? count($result['data']) : 0,
                    'attempt_duration_ms' => $attemptDuration,
                ]);

                // Log successful retry for diagnostics
                if ($attempt > 1 && $this->isDiagnosticLoggingEnabled()) {
                    $this->diagnosticLog('Partition retry succeeded', [
                        'context' => 'partition_retry_success',
                        'statement_id' => $statementId,
                        'partition' => $page,
                        'successful_attempt' => $attempt,
                        'total_retry_duration_ms' => round((microtime(true) - $retryStartTime) * 1000, 2),
                    ]);
                }

                return $result['data'] ?? [];

            } catch (Exception $e) {
                $errorMessage = $e->getMessage();
                $errorCode = $e->getCode();
                $isTransientError = $this->isTransientError($errorMessage, $errorCode);
                $attemptDuration = round((microtime(true) - $attemptStartTime) * 1000, 2);

                // Log detailed diagnostic information for every failure
                if ($this->isDiagnosticLoggingEnabled()) {
                    $diagnosticContext = [
                        'context' => 'partition_retry_failure',
                        'statement_id' => $statementId,
                        'partition' => $page,
                        'attempt' => $attempt,
                        'max_retries' => $maxRetries,
                        'error_message' => $errorMessage,
                        'error_code' => $errorCode,
                        'is_transient' => $isTransientError,
                        'attempt_duration_ms' => $attemptDuration,
                        'total_elapsed_ms' => round((microtime(true) - $retryStartTime) * 1000, 2),
                    ];

                    // Extract HTTP status from error message if present
                    if (preg_match('/HTTP\/\d(?:\.\d)?\s+(\d{3})/', $errorMessage, $matches)) {
                        $diagnosticContext['extracted_http_status'] = (int) $matches[1];
                    }

                    // Extract URL from error message if present
                    if (preg_match('/returned for "([^"]+)"/', $errorMessage, $matches)) {
                        $diagnosticContext['failed_url'] = $matches[1];
                    }

                    $this->diagnosticLog('Partition fetch attempt failed', $diagnosticContext);
                }

                $this->debugLog('SnowflakeService: Partition fetch failed', [
                    'page' => $page,
                    'attempt' => $attempt,
                    'error' => $errorMessage,
                    'is_transient' => $isTransientError,
                    'attempt_duration_ms' => $attemptDuration,
                ]);

                // Don't retry if it's not a transient error
                if (! $isTransientError) {
                    Log::error('SnowflakeService: Non-transient error fetching partition, not retrying', [
                        'page' => $page,
                        'error' => $errorMessage,
                        'error_code' => $errorCode,
                    ]);

                    throw $e;
                }

                // If this was the last attempt, log and throw
                if ($attempt >= $maxRetries) {
                    if ($this->isDiagnosticLoggingEnabled()) {
                        $this->diagnosticLog('All partition retry attempts exhausted', [
                            'context' => 'partition_retry_exhausted',
                            'statement_id' => $statementId,
                            'partition' => $page,
                            'total_attempts' => $maxRetries,
                            'total_duration_ms' => round((microtime(true) - $retryStartTime) * 1000, 2),
                            'final_error' => $errorMessage,
                        ]);
                    }

                    Log::error('SnowflakeService: All retry attempts failed for partition', [
                        'page' => $page,
                        'attempts' => $maxRetries,
                        'last_error' => $errorMessage,
                    ]);

                    // Re-throw on final failure so the error propagates
                    throw $e;
                }

                // Exponential backoff: 500ms, 1000ms, 2000ms, ...
                $delayMs = $baseDelayMs * pow(2, $attempt - 1);
                $this->debugLog('SnowflakeService: Waiting before retry', [
                    'delay_ms' => $delayMs,
                    'next_attempt' => $attempt + 1,
                ]);

                usleep($delayMs * 1000);
            }
        }

        return null;
    }

    /**
     * Determine if an error is transient and should be retried
     *
     * Transient errors include:
     * - HTTP 5xx errors (server-side failures)
     * - Connection reset/timeout errors
     * - Specific Snowflake maintenance errors
     *
     * @param  string  $errorMessage  The error message
     * @param  int  $errorCode  The error code
     * @return bool True if the error is transient
     */
    private function isTransientError(string $errorMessage, int $errorCode = 0): bool
    {
        // HTTP 5xx errors are transient
        if ($errorCode >= 500 && $errorCode < 600) {
            return true;
        }

        // Check error message for patterns indicating transient failures
        $transientPatterns = [
            'HTTP/2 5',           // HTTP/2 500, 502, 503, etc.
            'HTTP/1.1 5',         // HTTP/1.1 5xx errors
            '500 ',               // HTTP 500 in various formats
            '502 ',               // Bad Gateway
            '503 ',               // Service Unavailable
            '504 ',               // Gateway Timeout
            'connection reset',   // Connection issues
            'timed out',          // Timeout
            'temporarily unavailable',
            'service unavailable',
            'internal server error',
        ];

        $lowerMessage = strtolower($errorMessage);
        foreach ($transientPatterns as $pattern) {
            if (str_contains($lowerMessage, strtolower($pattern))) {
                return true;
            }
        }

        return false;
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
        // Capture raw response info BEFORE any processing
        $rawContent = null;
        $statusCode = null;
        $headers = [];

        try {
            $statusCode = $response->getStatusCode();
            $headers = $response->getHeaders(false);
            $rawContent = $response->getContent(false);
        } catch (Exception $captureError) {
            Log::error('SnowflakeService: Failed to capture raw response', [
                'error' => $captureError->getMessage(),
            ]);
        }

        try {
            if (empty($rawContent)) {
                throw new JsonException('Response body is empty.');
            }

            $content = $rawContent;

            // Handle gzip content
            if ('gzip' === ($headers['content-encoding'][0] ?? null)) {
                $content = $this->gzdecode($content);
            }

            // Remove problematic control characters while preserving valid JSON whitespace
            $content = $this->removeControlChars($content);

            // Decode with optimized options
            $data = json_decode($content, true, 512, JSON_BIGINT_AS_STRING | JSON_THROW_ON_ERROR);

            if ($statusCode >= 400) {
                // Log the raw response for any HTTP error
                Log::error('SnowflakeService: HTTP error response', [
                    'status_code' => $statusCode,
                    'snowflake_code' => $data['code'] ?? 'unknown',
                    'snowflake_message' => $data['message'] ?? 'Unknown error',
                    'response_body' => substr($content, 0, 2000),
                ]);

                throw new Exception(sprintf(
                    'Snowflake error, %s returned with message: %s',
                    $statusCode,
                    $data['message'] ?? 'Unknown error'
                ), $statusCode);
            }

            return $data;
        } catch (Exception $e) {
            // Always log raw response on ANY error
            Log::error('SnowflakeService: Response parsing failed - RAW RESPONSE', [
                'error' => $e->getMessage(),
                'error_code' => $e->getCode(),
                'http_status' => $statusCode,
                'content_type' => $headers['content-type'][0] ?? 'unknown',
                'content_encoding' => $headers['content-encoding'][0] ?? 'none',
                'raw_body_length' => $rawContent ? strlen($rawContent) : 0,
                'raw_body_preview' => $rawContent ? substr($rawContent, 0, 2000) : null,
                'looks_like_html' => $rawContent ? (preg_match('/<(!DOCTYPE|html)/i', $rawContent) === 1) : false,
            ]);

            $this->handleError($e, 'Error converting response to array');
            throw $e;
        }
    }

    /**
     * Remove problematic control characters from a string
     *
     * Removes control characters that are not valid in JSON:
     * - 0x00-0x08: NULL through BACKSPACE
     * - 0x0B: Vertical Tab
     * - 0x0C: Form Feed
     * - 0x0E-0x1F: Shift Out through Unit Separator
     * - 0x7F: DEL
     *
     * Preserves valid JSON whitespace:
     * - 0x09: Tab (\t)
     * - 0x0A: Line Feed (\n)
     * - 0x0D: Carriage Return (\r)
     *
     * @param  string  $content  The content to clean
     * @return string The cleaned content
     */
    private function removeControlChars(string $content): string
    {
        // Pattern matches control chars EXCEPT tab (0x09), LF (0x0A), CR (0x0D)
        // 0x00-0x08, 0x0B-0x0C, 0x0E-0x1F, 0x7F
        $pattern = '/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]/';

        $result = preg_replace($pattern, '', $content);

        // preg_replace returns null on error (e.g., PCRE backtrack limit exceeded)
        if ($result === null) {
            $this->debugLog('SnowflakeService: preg_replace failed, attempting fallback', [
                'content_length' => strlen($content),
                'preg_last_error' => preg_last_error(),
            ]);

            // Fallback: use str_replace for the most common problematic characters
            $controlChars = [];
            for ($i = 0; $i <= 8; $i++) {
                $controlChars[] = chr($i);
            }
            $controlChars[] = chr(0x0B); // Vertical tab
            $controlChars[] = chr(0x0C); // Form feed
            for ($i = 0x0E; $i <= 0x1F; $i++) {
                $controlChars[] = chr($i);
            }
            $controlChars[] = chr(0x7F); // DEL

            $result = str_replace($controlChars, '', $content);
        }

        return $result;
    }

    /**
     * Decompress gzipped content
     *
     * @param  string  $data  The compressed data (binary)
     * @return string The decompressed content
     */
    private function gzdecode(string $data): string
    {
        // First try native function which is most efficient
        $result = @gzdecode($data);
        if ($result !== false) {
            return $result;
        }

        // Fallback: use inflate_add with ZLIB_FINISH to decompress in one call
        // This is simpler and avoids the complexity of chunked processing
        $inflate = inflate_init(ZLIB_ENCODING_GZIP);
        if ($inflate === false) {
            throw new Exception('Failed to initialize inflate context');
        }

        $content = inflate_add($inflate, $data, ZLIB_FINISH);
        if ($content === false) {
            throw new Exception('Failed to decompress gzip data using inflate fallback');
        }

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

    /**
     * Check if diagnostic logging is enabled
     *
     * Diagnostic logging provides detailed response inspection for debugging
     * API issues. It is separate from debug logging and only enabled when
     * SNOWFLAKE_DIAGNOSTIC_LOGGING=true is set.
     */
    private function isDiagnosticLoggingEnabled(): bool
    {
        if ($this->diagnosticLoggingEnabled === null) {
            $this->diagnosticLoggingEnabled = filter_var(
                env('SNOWFLAKE_DIAGNOSTIC_LOGGING', false),
                FILTER_VALIDATE_BOOLEAN
            );
        }

        return $this->diagnosticLoggingEnabled;
    }

    /**
     * Log diagnostic information for troubleshooting
     *
     * This method logs detailed diagnostic information only when
     * SNOWFLAKE_DIAGNOSTIC_LOGGING=true. It automatically:
     * - Truncates large content fields to prevent log explosion
     * - Redacts sensitive data (tokens, keys)
     * - Uses a dedicated log channel if configured
     *
     * @param  string  $message  Log message
     * @param  array  $context  Additional context data
     */
    private function diagnosticLog(string $message, array $context = []): void
    {
        if (! $this->isDiagnosticLoggingEnabled()) {
            return;
        }

        // Truncate large fields
        $maxBodyLength = (int) env('SNOWFLAKE_DIAGNOSTIC_BODY_LIMIT', 2000);
        $safeContext = $this->truncateLargeFields($context, $maxBodyLength);

        // Redact sensitive data
        $safeContext = $this->redactSensitiveData($safeContext);

        // Use configured log channel or default
        $channel = env('SNOWFLAKE_DIAGNOSTIC_LOG_CHANNEL', null);

        if ($channel) {
            Log::channel($channel)->warning('SnowflakeService DIAGNOSTIC: '.$message, $safeContext);
        } else {
            Log::warning('SnowflakeService DIAGNOSTIC: '.$message, $safeContext);
        }
    }

    /**
     * Truncate large string fields in an array
     *
     * @param  array  $data  Data array to process
     * @param  int  $maxLength  Maximum length for string fields
     * @return array Processed array with truncated strings
     */
    private function truncateLargeFields(array $data, int $maxLength): array
    {
        $fieldsToTruncate = ['raw_body', 'raw_preview', 'content_preview', 'body', 'content'];

        foreach ($fieldsToTruncate as $field) {
            if (isset($data[$field]) && is_string($data[$field]) && strlen($data[$field]) > $maxLength) {
                $data[$field] = substr($data[$field], 0, $maxLength).'... [TRUNCATED, total length: '.strlen($data[$field]).']';
            }
        }

        return $data;
    }

    /**
     * Redact sensitive data from log context
     *
     * @param  array  $data  Data to redact
     * @return array Redacted data
     */
    private function redactSensitiveData(array $data): array
    {
        $sensitiveKeys = ['authorization', 'token', 'password', 'private_key', 'secret'];

        foreach ($data as $key => $value) {
            $lowerKey = strtolower($key);
            foreach ($sensitiveKeys as $sensitiveKey) {
                if (str_contains($lowerKey, $sensitiveKey)) {
                    $data[$key] = '[REDACTED]';
                    break;
                }
            }

            // Recursively process nested arrays
            if (is_array($value)) {
                $data[$key] = $this->redactSensitiveData($value);
            }
        }

        return $data;
    }

    /**
     * Capture detailed response information for diagnostics
     *
     * This method extracts all available information from an HTTP response
     * for debugging purposes. It safely handles responses that may be in
     * error states.
     *
     * @param  ResponseInterface  $response  The response to capture
     * @param  bool  $captureBody  Whether to capture the raw body
     * @return array Captured response details
     */
    private function captureResponseDetails(ResponseInterface $response, bool $captureBody = true): array
    {
        $details = [
            'timestamp' => date('Y-m-d H:i:s.u'),
        ];

        try {
            // Get response info (timing, URL, etc.) - always available
            $info = $response->getInfo();
            $details['url'] = $info['url'] ?? 'unknown';
            $details['http_code'] = $info['http_code'] ?? null;
            $details['total_time_ms'] = isset($info['total_time']) ? round($info['total_time'] * 1000, 2) : null;
            $details['connect_time_ms'] = isset($info['connect_time']) ? round($info['connect_time'] * 1000, 2) : null;
            $details['namelookup_time_ms'] = isset($info['namelookup_time']) ? round($info['namelookup_time'] * 1000, 2) : null;
            $details['redirect_count'] = $info['redirect_count'] ?? 0;
            $details['primary_ip'] = $info['primary_ip'] ?? 'unknown';
        } catch (Exception $e) {
            $details['info_error'] = $e->getMessage();
        }

        try {
            // Get status code
            $details['status_code'] = $response->getStatusCode();
        } catch (Exception $e) {
            $details['status_code_error'] = $e->getMessage();
        }

        try {
            // Get headers (false = don't throw on error)
            $headers = $response->getHeaders(false);
            $details['content_type'] = $headers['content-type'][0] ?? 'unknown';
            $details['content_encoding'] = $headers['content-encoding'][0] ?? 'none';
            $details['content_length'] = $headers['content-length'][0] ?? 'unknown';
            $details['x_snowflake_request_id'] = $headers['x-snowflake-request-id'][0] ?? 'unknown';

            // Capture all headers for full debugging
            $details['response_headers'] = $headers;
        } catch (Exception $e) {
            $details['headers_error'] = $e->getMessage();
        }

        if ($captureBody) {
            try {
                // Get raw body (false = don't throw on HTTP error status)
                $rawBody = $response->getContent(false);
                $details['raw_body_length'] = strlen($rawBody);
                $details['raw_preview'] = substr($rawBody, 0, 2000);

                // Detect response type
                $details['looks_like_html'] = preg_match('/<(!DOCTYPE|html)/i', $rawBody) === 1;
                $details['looks_like_json'] = isset($rawBody[0]) && ($rawBody[0] === '{' || $rawBody[0] === '[');
                $details['looks_like_gzip'] = isset($rawBody[0], $rawBody[1]) && $rawBody[0] === "\x1f" && $rawBody[1] === "\x8b";

                // If it looks like HTML, capture the title/error message
                if ($details['looks_like_html']) {
                    if (preg_match('/<title>([^<]+)<\/title>/i', $rawBody, $matches)) {
                        $details['html_title'] = $matches[1];
                    }
                    if (preg_match('/<h1[^>]*>([^<]+)<\/h1>/i', $rawBody, $matches)) {
                        $details['html_h1'] = $matches[1];
                    }
                }
            } catch (Exception $e) {
                $details['body_error'] = $e->getMessage();
            }
        }

        return $details;
    }
}
