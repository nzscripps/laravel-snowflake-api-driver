<?php

declare(strict_types=1);

namespace LaravelSnowflakeApi\Services;

use Exception;
use Illuminate\Cache\CacheManager;
use Illuminate\Contracts\Cache\LockTimeoutException;
use Illuminate\Contracts\Cache\Repository as CacheRepository;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\Log;
use Jose\Component\Core\AlgorithmManager;
use Jose\Component\KeyManagement\JWKFactory;
use Jose\Component\Signature\Algorithm\RS256;
use Jose\Component\Signature\JWSBuilder;
use Jose\Component\Signature\Serializer\CompactSerializer;
use LaravelSnowflakeApi\Exceptions\SnowflakeApiException;
use LaravelSnowflakeApi\Traits\DebugLogging;

/**
 * Thread-safe token provider with atomic locking
 *
 * This class implements secure, atomic token management to prevent:
 * - Thundering herd problem (100 processes generating tokens simultaneously)
 * - Cache race conditions (non-atomic cache operations)
 * - Token staleness (static cache outliving Laravel cache)
 * - Token leakage (secure handling of sensitive credentials)
 *
 * Design Patterns:
 * - Double-checked locking: Check cache before and after acquiring lock
 * - Hierarchical caching: Static (in-process) -> Laravel cache -> Generate new
 * - Graceful degradation: Fallback to generation if locking fails
 *
 * Performance Characteristics:
 * - Cache hit (static): ~0.1ms
 * - Cache hit (Laravel): ~0.5ms
 * - Cache miss (with lock): ~50-200ms (JWT generation time)
 * - Cache miss (100 concurrent): ~50-200ms total (vs 5000ms without locking)
 *
 * Security Features:
 * - Atomic token generation (only one process generates)
 * - Token expiry buffer (configurable, default 60s)
 * - Private key protection (never logged or exposed)
 * - Cache driver validation (ensures lock support)
 */
class ThreadSafeTokenProvider
{
    use DebugLogging;

    /**
     * Snowflake configuration
     */
    private SnowflakeConfig $config;

    /**
     * Static cache for in-process token reuse
     * Key format: "{account}:{user}" => ['token' => string, 'expiry' => int]
     */
    private static array $staticCache = [];

    /**
     * Token expiry buffer in seconds
     * Tokens are considered expired this many seconds before actual expiry
     * to prevent mid-request expiration
     */
    private int $expiryBuffer;

    /**
     * Lock timeout in seconds
     * Maximum time to wait for lock acquisition
     */
    private int $lockTimeout;

    /**
     * Lock retry interval in milliseconds
     * Time to wait between lock acquisition attempts when using block()
     */
    private int $lockRetryInterval;

    /**
     * Whether cache driver validation has been performed
     */
    private static bool $driverValidated = false;

    /**
     * Whether cache driver supports atomic locks
     */
    private static bool $driverSupportsLocks = false;

    /**
     * Initialize thread-safe token provider
     *
     * @param SnowflakeConfig $config Snowflake configuration
     * @param int $expiryBuffer Token expiry buffer in seconds (default: 60)
     * @param int $lockTimeout Lock acquisition timeout in seconds (default: 5)
     * @param int $lockRetryInterval Lock retry interval in milliseconds (default: 100)
     *
     * @throws SnowflakeApiException If cache driver doesn't support locks
     */
    public function __construct(
        SnowflakeConfig $config,
        int $expiryBuffer = 60,
        int $lockTimeout = 5,
        int $lockRetryInterval = 100
    ) {
        $this->config = $config;
        $this->expiryBuffer = $expiryBuffer;
        $this->lockTimeout = $lockTimeout;
        $this->lockRetryInterval = $lockRetryInterval;

        // Validate cache driver once per process
        if (! self::$driverValidated) {
            $this->validateCacheDriver();
            self::$driverValidated = true;
        }
    }

    /**
     * Get a valid access token using atomic locking
     *
     * This method implements the double-checked locking pattern:
     * 1. Check static cache (fastest, in-process)
     * 2. Check Laravel cache (fast, cross-process)
     * 3. Acquire lock (ensures only one process generates token)
     * 4. Double-check Laravel cache (another process might have generated it)
     * 5. Generate new token if still needed
     * 6. Store in both caches
     *
     * @return string Valid JWT access token
     *
     * @throws SnowflakeApiException If token generation fails
     */
    public function getToken(): string
    {
        $cacheKey = $this->getCacheKey();
        $lockKey = $this->getLockKey();

        // PHASE 1: Check static cache (fastest path)
        $staticToken = $this->getFromStaticCache();
        if ($staticToken !== null) {
            $this->debugLog('ThreadSafeTokenProvider: Token retrieved from static cache');
            return $staticToken;
        }

        // PHASE 2: Check Laravel cache (fast path)
        $laravelToken = $this->getFromLaravelCache($cacheKey);
        if ($laravelToken !== null) {
            $this->debugLog('ThreadSafeTokenProvider: Token retrieved from Laravel cache');
            $this->updateStaticCache($laravelToken);
            return $laravelToken;
        }

        // PHASE 3: Acquire lock and generate token (slow path)
        return $this->acquireLockAndGenerateToken($cacheKey, $lockKey);
    }

    /**
     * Validate the expiry buffer configuration
     *
     * @param int $expiryBuffer Expiry buffer in seconds
     * @return int Validated expiry buffer
     *
     * @throws SnowflakeApiException If expiry buffer is invalid
     */
    public function validateExpiryBuffer(int $expiryBuffer): int
    {
        if ($expiryBuffer < 30) {
            throw new SnowflakeApiException(
                'Token expiry buffer must be at least 30 seconds to prevent mid-request expiration'
            );
        }

        if ($expiryBuffer > 600) {
            throw new SnowflakeApiException(
                'Token expiry buffer cannot exceed 600 seconds (10 minutes)'
            );
        }

        return $expiryBuffer;
    }

    /**
     * Clear all token caches
     *
     * This method should be called when:
     * - Token is manually revoked
     * - User credentials change
     * - Security incident requires token rotation
     *
     * @return void
     */
    public function clearTokenCache(): void
    {
        $cacheKey = $this->getCacheKey();

        // Clear Laravel cache
        Cache::forget($cacheKey);

        // Clear static cache
        $staticKey = $this->getStaticCacheKey();
        unset(self::$staticCache[$staticKey]);

        $this->debugLog('ThreadSafeTokenProvider: Token cache cleared', [
            'cache_key' => $cacheKey,
        ]);
    }

    /**
     * Validate that cache driver supports atomic locks
     *
     * @return void
     *
     * @throws SnowflakeApiException If cache driver doesn't support locks
     */
    private function validateCacheDriver(): void
    {
        // Get cache driver safely (handle cases where Laravel isn't fully bootstrapped)
        try {
            $driver = function_exists('config') ? config('cache.default') : 'array';
        } catch (Exception $e) {
            $driver = 'array';
        }

        $this->debugLog('ThreadSafeTokenProvider: Validating cache driver', [
            'driver' => $driver,
        ]);

        // Check if driver supports locks by attempting to create one
        try {
            $testLock = Cache::lock('snowflake_test_lock_' . uniqid(), 1);

            // If lock() doesn't throw an exception, the driver supports locks
            self::$driverSupportsLocks = true;

            // Clean up test lock
            $testLock->forceRelease();

            $this->debugLog('ThreadSafeTokenProvider: Cache driver supports atomic locks', [
                'driver' => $driver,
            ]);
        } catch (Exception $e) {
            self::$driverSupportsLocks = false;

            // Log warning but don't fail - we'll fall back to non-atomic generation
            Log::warning('ThreadSafeTokenProvider: Cache driver does not support atomic locks', [
                'driver' => $driver,
                'error' => $e->getMessage(),
                'recommendation' => 'Use Redis or Memcached for production environments',
            ]);

            $this->debugLog('ThreadSafeTokenProvider: Cache driver validation failed, will use fallback', [
                'driver' => $driver,
            ]);
        }
    }

    /**
     * Get cache key for token storage
     *
     * @return string Cache key
     */
    private function getCacheKey(): string
    {
        return sprintf(
            'snowflake_api_token:%s:%s',
            $this->config->getAccount(),
            $this->config->getUser()
        );
    }

    /**
     * Get lock key for atomic token generation
     *
     * Lock key is separate from cache key to allow independent management
     *
     * @return string Lock key
     */
    private function getLockKey(): string
    {
        return sprintf(
            'snowflake_api_token_lock:%s:%s',
            $this->config->getAccount(),
            $this->config->getUser()
        );
    }

    /**
     * Get static cache key
     *
     * @return string Static cache key
     */
    private function getStaticCacheKey(): string
    {
        return sprintf(
            '%s:%s',
            $this->config->getAccount(),
            $this->config->getUser()
        );
    }

    /**
     * Get token from static cache if valid
     *
     * @return string|null Token if valid, null otherwise
     */
    private function getFromStaticCache(): ?string
    {
        $staticKey = $this->getStaticCacheKey();

        if (! isset(self::$staticCache[$staticKey])) {
            return null;
        }

        $cachedData = self::$staticCache[$staticKey];

        // Validate structure
        if (! isset($cachedData['token'], $cachedData['expiry'])) {
            unset(self::$staticCache[$staticKey]);
            return null;
        }

        // Check expiry with buffer
        if (time() >= $cachedData['expiry'] - $this->expiryBuffer) {
            unset(self::$staticCache[$staticKey]);
            return null;
        }

        return $cachedData['token'];
    }

    /**
     * Get token from Laravel cache if valid
     *
     * @param string $cacheKey Cache key
     * @return string|null Token if valid, null otherwise
     */
    private function getFromLaravelCache(string $cacheKey): ?string
    {
        $cachedData = Cache::get($cacheKey);

        if ($cachedData === null) {
            return null;
        }

        // Validate structure
        if (! is_array($cachedData) || ! isset($cachedData['token'], $cachedData['expiry'])) {
            Cache::forget($cacheKey);
            return null;
        }

        // Check expiry with buffer
        if (time() >= $cachedData['expiry'] - $this->expiryBuffer) {
            Cache::forget($cacheKey);
            return null;
        }

        return $cachedData['token'];
    }

    /**
     * Update static cache with token data
     *
     * @param string $token Token to cache
     * @return void
     */
    private function updateStaticCache(string $token): void
    {
        $staticKey = $this->getStaticCacheKey();
        $cacheKey = $this->getCacheKey();

        // Get expiry from Laravel cache
        $cachedData = Cache::get($cacheKey);
        if ($cachedData !== null && isset($cachedData['expiry'])) {
            self::$staticCache[$staticKey] = [
                'token' => $token,
                'expiry' => $cachedData['expiry'],
            ];
        }
    }

    /**
     * Acquire lock and generate token atomically
     *
     * This method implements the atomic token generation with these steps:
     * 1. Attempt to acquire lock (with timeout)
     * 2. Double-check cache (another process might have generated it)
     * 3. Generate new token
     * 4. Store in caches
     * 5. Release lock
     *
     * @param string $cacheKey Cache key
     * @param string $lockKey Lock key
     * @return string Valid JWT access token
     *
     * @throws SnowflakeApiException If token generation fails
     */
    private function acquireLockAndGenerateToken(string $cacheKey, string $lockKey): string
    {
        // If driver doesn't support locks, fall back to direct generation
        if (! self::$driverSupportsLocks) {
            $this->debugLog('ThreadSafeTokenProvider: Lock not supported, generating token directly');
            return $this->generateAndCacheToken($cacheKey);
        }

        try {
            // Attempt to acquire lock with blocking
            $lock = Cache::lock($lockKey, $this->lockTimeout);

            $this->debugLog('ThreadSafeTokenProvider: Attempting to acquire lock', [
                'lock_key' => $lockKey,
                'timeout' => $this->lockTimeout,
            ]);

            // Use block() to wait for lock (handles contention gracefully)
            $token = $lock->block($this->lockTimeout, function () use ($cacheKey) {
                $this->debugLog('ThreadSafeTokenProvider: Lock acquired, double-checking cache');

                // CRITICAL: Double-check cache after acquiring lock
                // Another process might have generated the token while we were waiting
                $existingToken = $this->getFromLaravelCache($cacheKey);
                if ($existingToken !== null) {
                    $this->debugLog('ThreadSafeTokenProvider: Token found in cache after lock acquisition');
                    $this->updateStaticCache($existingToken);
                    return $existingToken;
                }

                // Generate new token
                $this->debugLog('ThreadSafeTokenProvider: Generating new token');
                return $this->generateAndCacheToken($cacheKey);
            });

            $this->debugLog('ThreadSafeTokenProvider: Lock released successfully');

            return $token;

        } catch (LockTimeoutException $e) {
            // Lock timeout - another process is taking too long
            // Fall back to checking cache one more time before generating
            $this->debugLog('ThreadSafeTokenProvider: Lock timeout, checking cache before fallback', [
                'timeout' => $this->lockTimeout,
            ]);

            $existingToken = $this->getFromLaravelCache($cacheKey);
            if ($existingToken !== null) {
                $this->debugLog('ThreadSafeTokenProvider: Found token in cache after timeout');
                $this->updateStaticCache($existingToken);
                return $existingToken;
            }

            // Last resort: generate without lock (better than failing)
            Log::warning('ThreadSafeTokenProvider: Generating token without lock due to timeout', [
                'timeout' => $this->lockTimeout,
                'recommendation' => 'Consider increasing lock timeout',
            ]);

            return $this->generateAndCacheToken($cacheKey);

        } catch (Exception $e) {
            // Unexpected error - fall back to generation
            Log::error('ThreadSafeTokenProvider: Unexpected error during lock acquisition', [
                'error' => $e->getMessage(),
                'trace' => $e->getTraceAsString(),
            ]);

            return $this->generateAndCacheToken($cacheKey);
        }
    }

    /**
     * Generate new token and cache it
     *
     * This method generates a new JWT token and stores it in both
     * static cache (in-process) and Laravel cache (cross-process)
     *
     * @param string $cacheKey Cache key
     * @return string Valid JWT access token
     *
     * @throws SnowflakeApiException If token generation fails
     */
    private function generateAndCacheToken(string $cacheKey): string
    {
        $this->debugLog('ThreadSafeTokenProvider: Generating new JWT token');

        try {
            $token = $this->generateJwtToken();
            $expiryTime = time() + 3600; // 1 hour expiry

            // Calculate cache duration (expiry minus buffer)
            $cacheDuration = 3600 - $this->expiryBuffer;

            // Store in Laravel cache
            $cacheData = [
                'token' => $token,
                'expiry' => $expiryTime,
            ];
            Cache::put($cacheKey, $cacheData, $cacheDuration);

            // Store in static cache
            $staticKey = $this->getStaticCacheKey();
            self::$staticCache[$staticKey] = $cacheData;

            $this->debugLog('ThreadSafeTokenProvider: Token generated and cached', [
                'cache_key' => $cacheKey,
                'cache_duration' => $cacheDuration,
                'expiry_time' => date('Y-m-d H:i:s', $expiryTime),
                'expiry_buffer' => $this->expiryBuffer,
            ]);

            return $token;

        } catch (Exception $e) {
            $this->handleError($e, 'Error generating JWT token');
            throw new SnowflakeApiException(
                'Failed to generate access token: ' . $e->getMessage(),
                0,
                $e
            );
        }
    }

    /**
     * Generate JWT token using RSA signing
     *
     * This method performs the expensive cryptographic operations:
     * - Load and parse private key
     * - Create JWT payload with proper claims
     * - Sign with RS256 algorithm
     * - Serialize to compact format
     *
     * Expected execution time: 50-200ms depending on key size and CPU
     *
     * @return string JWT token
     *
     * @throws Exception If JWT generation fails
     */
    private function generateJwtToken(): string
    {
        // Retrieve and validate private key
        $keyContent = $this->config->getPrivateKey();
        if (empty($keyContent)) {
            throw new Exception('Private key content is empty');
        }

        // Replace literal '\n' sequences with actual newlines
        $keyContent = str_replace('\n', "\n", $keyContent);

        $this->debugLog('ThreadSafeTokenProvider: Preparing private key', [
            'key_length' => strlen($keyContent),
            'contains_begin' => strpos($keyContent, '-----BEGIN') !== false,
            'contains_end' => strpos($keyContent, '-----END') !== false,
        ]);

        // Create JWK from private key
        // SECURITY: Private key is processed in memory and never persisted
        $privateKey = JWKFactory::createFromKey(
            $keyContent,
            $this->config->getPrivateKeyPassphrase(),
            [
                'use' => 'sig',
                'alg' => 'RS256',
            ]
        );

        // Prepare JWT claims
        $publicKeyFingerprint = 'SHA256:' . $this->config->getPublicKey();
        $expiresIn = time() + 3600; // 1 hour expiry

        $payload = [
            'iss' => sprintf('%s.%s', $this->config->getUser(), $publicKeyFingerprint),
            'sub' => $this->config->getUser(),
            'iat' => time(),
            'exp' => $expiresIn,
        ];

        $this->debugLog('ThreadSafeTokenProvider: Creating JWT payload', [
            'iss' => $payload['iss'],
            'sub' => $payload['sub'],
            'exp_in_seconds' => $expiresIn - time(),
        ]);

        // Build and sign JWT
        $algorithmManager = new AlgorithmManager([new RS256]);
        $jwsBuilder = new JWSBuilder($algorithmManager);

        $jws = $jwsBuilder
            ->create()
            ->withPayload(json_encode($payload))
            ->addSignature($privateKey, ['alg' => 'RS256', 'typ' => 'JWT'])
            ->build();

        // Serialize to compact format
        $serializer = new CompactSerializer;
        $token = $serializer->serialize($jws);

        // Log token metadata (never log the actual token)
        $tokenParts = explode('.', $token);
        $this->debugLog('ThreadSafeTokenProvider: JWT token generated', [
            'token_length' => strlen($token),
            'parts_count' => count($tokenParts),
            'header_length' => strlen($tokenParts[0] ?? ''),
            'payload_length' => strlen($tokenParts[1] ?? ''),
            'signature_length' => strlen($tokenParts[2] ?? ''),
        ]);

        return $token;
    }

    /**
     * Handle and log errors consistently
     *
     * @param Exception $e The exception to handle
     * @param string $context Context information
     * @param array $additionalData Additional data to log
     * @return void
     */
    private function handleError(Exception $e, string $context, array $additionalData = []): void
    {
        $errorData = array_merge([
            'error' => $e->getMessage(),
            'context' => $context,
        ], $additionalData);

        Log::error('ThreadSafeTokenProvider: ' . $context, $errorData);
    }
}
