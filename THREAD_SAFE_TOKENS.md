# Thread-Safe Token Management - Implementation Guide

## Overview

This guide explains the thread-safe token management system implemented in the Laravel Snowflake API Driver. The system prevents the "thundering herd" problem and ensures tokens are never leaked across requests.

## Problem Statement

### Before: Race Conditions and Thundering Herd

```
Timeline with 100 concurrent requests when token expires:

T1: Process A checks cache → NULL (expired)
T2: Process B checks cache → NULL (expired)
...
T100: Process Z checks cache → NULL (expired)

ALL 100 PROCESSES: Generate JWT tokens simultaneously
- 100x RSA signing operations (CPU-intensive)
- Total time: ~5000ms (50ms × 100)
- Server load spike
- Cache stampede
```

### After: Atomic Token Generation

```
Timeline with 100 concurrent requests when token expires:

T1: Process A checks cache → NULL (expired)
T2: Process A acquires lock
T3: Processes B-Z check cache → NULL, wait for lock
T4: Process A generates token (50ms)
T5: Process A stores token in cache
T6: Process A releases lock
T7: Processes B-Z acquire lock, check cache → HIT!
T8: Processes B-Z return cached token

ONLY 1 PROCESS: Generates token
- 1x RSA signing operation
- Total time: ~50ms
- 99% time reduction
- No cache stampede
```

## Architecture

### Component Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    Application Layer                         │
│  (SnowflakeService, SnowflakeApiConnection, Eloquent ORM)   │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│             ThreadSafeTokenProvider                          │
│  - Atomic token generation                                   │
│  - Double-checked locking                                    │
│  - Hierarchical caching                                      │
│  - Graceful degradation                                      │
└─────────────────────┬───────────────────────────────────────┘
                      │
        ┌─────────────┼─────────────┐
        │             │             │
        ▼             ▼             ▼
┌──────────────┐ ┌──────────┐ ┌─────────────┐
│Static Cache  │ │Laravel   │ │Distributed  │
│(in-process)  │ │Cache     │ │Lock         │
│~0.1ms        │ │~0.5ms    │ │~1-5ms       │
└──────────────┘ └──────────┘ └─────────────┘
                      │             │
                      └──────┬──────┘
                             ▼
                      ┌──────────────┐
                      │Cache Backend │
                      │(Redis/       │
                      │ Memcached)   │
                      └──────────────┘
```

### Token Retrieval Flow

```
getToken()
    │
    ├─→ Check Static Cache
    │       │
    │       ├─→ HIT: Return token (0.1ms)
    │       │
    │       └─→ MISS: Continue
    │
    ├─→ Check Laravel Cache
    │       │
    │       ├─→ HIT: Update static cache, return token (0.5ms)
    │       │
    │       └─→ MISS: Continue
    │
    ├─→ Acquire Distributed Lock
    │       │
    │       ├─→ SUCCESS:
    │       │       │
    │       │       ├─→ Double-check Laravel Cache
    │       │       │       │
    │       │       │       ├─→ HIT: Return cached token
    │       │       │       │
    │       │       │       └─→ MISS: Generate token
    │       │       │
    │       │       └─→ Store in caches, release lock
    │       │
    │       └─→ TIMEOUT:
    │               │
    │               ├─→ Check cache one more time
    │               │       │
    │               │       ├─→ HIT: Return cached token
    │               │       │
    │               │       └─→ MISS: Generate without lock
    │               │
    │               └─→ Return token
    │
    └─→ Return token
```

## API Reference

### ThreadSafeTokenProvider

```php
namespace LaravelSnowflakeApi\Services;

class ThreadSafeTokenProvider
{
    /**
     * Create a new token provider
     *
     * @param SnowflakeConfig $config Snowflake configuration
     * @param int $expiryBuffer Token expiry buffer in seconds (default: 60)
     * @param int $lockTimeout Lock acquisition timeout in seconds (default: 5)
     * @param int $lockRetryInterval Lock retry interval in ms (default: 100)
     */
    public function __construct(
        SnowflakeConfig $config,
        int $expiryBuffer = 60,
        int $lockTimeout = 5,
        int $lockRetryInterval = 100
    );

    /**
     * Get a valid access token
     *
     * This method implements atomic token generation with:
     * - Static cache check (fastest)
     * - Laravel cache check (fast)
     * - Distributed lock acquisition (atomic)
     * - Double-checked locking (efficient)
     * - Graceful degradation (reliable)
     *
     * @return string Valid JWT access token
     * @throws SnowflakeApiException If token generation fails
     */
    public function getToken(): string;

    /**
     * Clear all token caches
     *
     * Call this when:
     * - Token is manually revoked
     * - User credentials change
     * - Security incident requires token rotation
     *
     * @return void
     */
    public function clearTokenCache(): void;

    /**
     * Validate expiry buffer configuration
     *
     * @param int $expiryBuffer Expiry buffer in seconds
     * @return int Validated expiry buffer
     * @throws SnowflakeApiException If buffer is invalid
     */
    public function validateExpiryBuffer(int $expiryBuffer): int;
}
```

### Configuration Options

```php
// Default configuration (recommended for most use cases)
$provider = new ThreadSafeTokenProvider($config);

// Custom expiry buffer (for high-latency networks)
$provider = new ThreadSafeTokenProvider(
    $config,
    $expiryBuffer = 120 // 2 minutes
);

// Custom lock timeout (for slow token generation)
$provider = new ThreadSafeTokenProvider(
    $config,
    $expiryBuffer = 60,
    $lockTimeout = 10 // 10 seconds
);

// Full customization
$provider = new ThreadSafeTokenProvider(
    $config,
    $expiryBuffer = 120,      // 2 minutes before expiry
    $lockTimeout = 10,        // 10 seconds to acquire lock
    $lockRetryInterval = 200  // Check lock every 200ms
);
```

## Performance Analysis

### Cache Hit Scenario (95% of requests)

```
Operation: getToken()
Steps:
  1. Check static cache: 0.1ms
  2. Return token: 0.1ms
Total: 0.1ms

Throughput: ~10,000 requests/second/core
```

### Cache Miss with Lock (5% of requests)

```
Operation: getToken() [first process]
Steps:
  1. Check static cache: 0.1ms
  2. Check Laravel cache: 0.5ms
  3. Acquire lock: 1-5ms
  4. Double-check cache: 0.5ms
  5. Generate JWT: 50-200ms (RSA signing)
  6. Store in caches: 0.5ms
  7. Release lock: 0.1ms
Total: 52-207ms

Operation: getToken() [concurrent processes]
Steps:
  1. Check static cache: 0.1ms
  2. Check Laravel cache: 0.5ms
  3. Wait for lock: 1-50ms (depends on first process)
  4. Acquire lock: 1-5ms
  5. Double-check cache: 0.5ms (HIT!)
  6. Return cached token: 0.1ms
Total: 3-57ms (NO JWT generation!)
```

### Thundering Herd Prevention

```
Scenario: 100 concurrent requests when token expires

WITHOUT atomic locking:
- 100 processes generate tokens simultaneously
- 100 × 50ms = 5000ms total CPU time
- Server load spike
- Cache stampede

WITH atomic locking:
- 1 process generates token: 50ms
- 99 processes wait and use cached token: 3-57ms each
- Total time: 50-60ms (99% reduction)
- Smooth server load
- No cache stampede
```

## Configuration Guide

### Cache Driver Setup

#### Redis (Recommended)

```php
// config/cache.php
'default' => 'redis',

'stores' => [
    'redis' => [
        'driver' => 'redis',
        'connection' => 'cache',
        'lock_connection' => 'default', // Important for atomic locks
    ],
],

// .env
CACHE_DRIVER=redis
REDIS_HOST=127.0.0.1
REDIS_PASSWORD=your-secure-password
REDIS_PORT=6379
```

**Benefits**:
- Full atomic lock support (SETNX command)
- Excellent performance (~0.5ms operations)
- Distributed across servers
- Production-ready

#### Memcached (Acceptable)

```php
// config/cache.php
'default' => 'memcached',

'stores' => [
    'memcached' => [
        'driver' => 'memcached',
        'servers' => [
            [
                'host' => env('MEMCACHED_HOST', '127.0.0.1'),
                'port' => env('MEMCACHED_PORT', 11211),
                'weight' => 100,
            ],
        ],
    ],
],

// .env
CACHE_DRIVER=memcached
MEMCACHED_HOST=127.0.0.1
MEMCACHED_PORT=11211
```

**Benefits**:
- Atomic lock support (CAS command)
- Good performance
- Distributed across servers

#### Database (Works but Slower)

```php
// config/cache.php
'default' => 'database',

'stores' => [
    'database' => [
        'driver' => 'database',
        'table' => 'cache',
        'connection' => null,
        'lock_connection' => null,
    ],
],

// .env
CACHE_DRIVER=database
```

**Note**: Run migration first:
```bash
php artisan cache:table
php artisan migrate
```

**Limitations**:
- Slower than Redis/Memcached (~5-10ms operations)
- Lock support depends on database engine
- Not recommended for high-traffic scenarios

### Expiry Buffer Configuration

```php
// config/snowflake.php (create this file)

return [
    'token' => [
        // Token expiry buffer in seconds
        // Tokens are considered expired this many seconds before actual expiry
        // to prevent mid-request expiration
        'expiry_buffer' => env('SNOWFLAKE_TOKEN_EXPIRY_BUFFER', 60),

        // Lock timeout in seconds
        // Maximum time to wait for lock acquisition
        'lock_timeout' => env('SNOWFLAKE_TOKEN_LOCK_TIMEOUT', 5),

        // Lock retry interval in milliseconds
        // Time to wait between lock acquisition attempts
        'lock_retry_interval' => env('SNOWFLAKE_TOKEN_LOCK_RETRY_INTERVAL', 100),
    ],
];
```

```bash
# .env

# Token expires 60 seconds before actual expiry
SNOWFLAKE_TOKEN_EXPIRY_BUFFER=60

# Wait up to 5 seconds to acquire lock
SNOWFLAKE_TOKEN_LOCK_TIMEOUT=5

# Check lock every 100ms
SNOWFLAKE_TOKEN_LOCK_RETRY_INTERVAL=100
```

## Edge Cases & Solutions

### Edge Case 1: Token Expires Mid-Request

**Problem**:
```
Timeline:
T1: Request gets token (expires at 14:00:00)
T2: Request starts API call at 13:59:59
T3: API call reaches Snowflake at 14:00:01 (EXPIRED!)
```

**Solution**: Expiry buffer
```php
$expiryBuffer = 60; // Consider expired 60s before actual expiry

// Token expires at 14:00:00
// We consider it expired at 13:59:00
// All requests after 13:59:00 get a new token
// API calls complete safely before 14:00:00
```

### Edge Case 2: Token Revoked Externally

**Problem**:
```
Timeline:
T1: Token cached in static and Laravel cache
T2: Admin revokes token in Snowflake console
T3: Next request uses cached token (INVALID!)
```

**Solution**: Clear cache manually
```php
// In your admin controller
$service = app(SnowflakeService::class);
$service->clearTokenCache(); // Add this method if needed

// Or directly
$provider = new ThreadSafeTokenProvider($config);
$provider->clearTokenCache();
```

**Automatic Detection**:
```php
// The driver will receive 401 Unauthorized from Snowflake
// Catch this and clear cache automatically

try {
    $result = $service->ExecuteQuery($query);
} catch (SnowflakeApiException $e) {
    if ($e->getCode() === 401) {
        // Token invalid, clear cache and retry
        $provider->clearTokenCache();
        $result = $service->ExecuteQuery($query);
    }
}
```

### Edge Case 3: Clock Skew

**Problem**:
```
Timeline:
T1: Server A clock is 5 minutes ahead
T2: Server A thinks token expired, generates new one
T3: Server B still uses old token (conflict)
```

**Solution**: NTP synchronization + expiry buffer
```bash
# Install NTP
sudo apt-get install ntp

# Configure NTP
sudo nano /etc/ntp.conf

# Monitor clock offset
ntpq -p
```

**Buffer Accommodation**:
- 60-second expiry buffer accommodates minor clock skew (<60s)
- If clock skew >60s, increase buffer:
```php
$provider = new ThreadSafeTokenProvider($config, $expiryBuffer = 120);
```

### Edge Case 4: Cache Backend Failure

**Problem**:
```
Timeline:
T1: Redis crashes
T2: All requests fall back to direct token generation
T3: Thundering herd problem returns (100 concurrent generations)
```

**Solution**: Graceful degradation + monitoring
```php
// The system automatically falls back to non-atomic generation
// This prevents complete failure but reduces performance

// Monitor cache health
if (! Cache::getStore()->getRedis()->ping()) {
    Log::alert('Redis cache is down - token generation is not atomic');
    // Alert operations team
}
```

**Circuit Breaker Pattern** (optional):
```php
class CircuitBreakerTokenProvider extends ThreadSafeTokenProvider
{
    private int $failureCount = 0;
    private int $failureThreshold = 5;
    private bool $circuitOpen = false;

    public function getToken(): string
    {
        if ($this->circuitOpen) {
            // Circuit open - skip locking, generate directly
            return $this->generateAndCacheToken($this->getCacheKey());
        }

        try {
            return parent::getToken();
        } catch (Exception $e) {
            $this->failureCount++;
            if ($this->failureCount >= $this->failureThreshold) {
                $this->circuitOpen = true;
                Log::alert('Token provider circuit breaker opened');
            }
            throw $e;
        }
    }
}
```

## Migration Guide

### From Existing SnowflakeService

The migration is **automatic** - no code changes required in your application.

**Before** (v1.x):
```php
// SnowflakeService.php
private function getAccessToken(): string
{
    // Old implementation with race conditions
    // Non-atomic cache operations
    // Potential thundering herd
}
```

**After** (v2.x):
```php
// SnowflakeService.php
private function getAccessToken(): string
{
    return $this->tokenProvider->getToken();
}

// ThreadSafeTokenProvider.php
public function getToken(): string
{
    // New implementation with atomic locks
    // Double-checked locking
    // Thundering herd prevention
}
```

### Backward Compatibility

**100% backward compatible** - existing code works without changes:

```php
// Your existing code
$connection = DB::connection('snowflake_api');
$results = $connection->table('users')->get();

// Still works! Token management is transparent
```

### Configuration Migration

**Optional**: Add cache driver validation warning suppression:

```php
// config/logging.php
'channels' => [
    'snowflake' => [
        'driver' => 'single',
        'path' => storage_path('logs/snowflake.log'),
        'level' => 'info',
    ],
],
```

### Testing Migration

**Run existing tests** - they should all pass:

```bash
# Unit tests (no credentials needed)
composer test:unit

# Integration tests (requires credentials)
composer test:integration
```

**New tests** verify thread-safe behavior:

```bash
# Run thread-safe token provider tests
./vendor/bin/phpunit tests/Unit/Services/ThreadSafeTokenProviderTest.php
./vendor/bin/phpunit tests/Integration/ThreadSafeTokenProviderIntegrationTest.php
```

## Monitoring & Alerting

### Key Metrics

```php
// Monitor token generation rate
Log::info('Token generated', [
    'account' => $account,
    'user' => $user,
    'generation_time_ms' => $generationTime,
]);

// Monitor cache hit rate
Log::info('Token retrieved', [
    'source' => 'static_cache', // or 'laravel_cache' or 'generated'
    'retrieval_time_ms' => $retrievalTime,
]);

// Monitor lock timeouts
Log::warning('Lock timeout', [
    'lock_key' => $lockKey,
    'timeout_seconds' => $lockTimeout,
]);
```

### Recommended Alerts

```yaml
# Prometheus-style alerts

- alert: HighTokenGenerationRate
  expr: rate(snowflake_token_generated_total[5m]) > 10
  for: 5m
  annotations:
    summary: High token generation rate detected
    description: "{{ $value }} tokens/min (expected <1/min)"

- alert: LowCacheHitRate
  expr: snowflake_cache_hit_rate < 0.95
  for: 10m
  annotations:
    summary: Low cache hit rate
    description: "{{ $value }}% (expected >95%)"

- alert: LockTimeouts
  expr: rate(snowflake_lock_timeout_total[5m]) > 0
  for: 5m
  annotations:
    summary: Token lock timeouts occurring
    description: "{{ $value }} timeouts/min"

- alert: CacheBackendDown
  expr: up{job="redis"} == 0
  for: 1m
  annotations:
    summary: Cache backend is down
    description: Token generation is not atomic
```

### Dashboard Example

```
┌─────────────────────────────────────────────────────────┐
│  Snowflake Token Management Dashboard                   │
├─────────────────────────────────────────────────────────┤
│                                                          │
│  Token Generation Rate: 0.8 / min       ✓ Healthy       │
│  Cache Hit Rate:        98.5%           ✓ Healthy       │
│  Lock Timeout Rate:     0.0%            ✓ Healthy       │
│  Avg Generation Time:   52ms            ✓ Normal        │
│                                                          │
│  ┌──────────────────────────────────────────────────┐  │
│  │ Cache Hit Sources (Last Hour)                    │  │
│  │                                                   │  │
│  │  Static Cache:  95% ████████████████████████████ │  │
│  │  Laravel Cache:  3% ██                           │  │
│  │  Generated:      2% █                            │  │
│  └──────────────────────────────────────────────────┘  │
│                                                          │
│  ┌──────────────────────────────────────────────────┐  │
│  │ Token Generation Time (P50/P95/P99)              │  │
│  │                                                   │  │
│  │  P50: 48ms   ████████████                        │  │
│  │  P95: 72ms   ██████████████████                  │  │
│  │  P99: 120ms  ███████████████████████████         │  │
│  └──────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
```

## Troubleshooting

### High Token Generation Rate

**Symptoms**: More than 10 tokens generated per minute

**Possible Causes**:
1. Cache backend is down or slow
2. Cache hit rate is low
3. Many unique account/user combinations
4. Expiry buffer too large

**Solutions**:
```bash
# Check cache health
redis-cli ping

# Check cache hit rate
tail -f storage/logs/laravel.log | grep "Token retrieved"

# Reduce expiry buffer if too large
SNOWFLAKE_TOKEN_EXPIRY_BUFFER=60 # instead of 300
```

### Lock Timeouts

**Symptoms**: Frequent "Lock timeout" warnings in logs

**Possible Causes**:
1. Token generation is very slow (>5 seconds)
2. High concurrency
3. Lock timeout too short

**Solutions**:
```php
// Increase lock timeout
$provider = new ThreadSafeTokenProvider($config, 60, $lockTimeout = 10);

// Or in config
SNOWFLAKE_TOKEN_LOCK_TIMEOUT=10
```

### Cache Not Working

**Symptoms**: Every request generates a new token

**Possible Causes**:
1. Cache driver not configured correctly
2. Cache driver doesn't persist (using 'array')
3. Expiry buffer equals token lifetime

**Solutions**:
```bash
# Check cache configuration
php artisan config:cache
php artisan cache:clear

# Verify cache is working
php artisan tinker
>>> Cache::put('test', 'value', 60);
>>> Cache::get('test'); // Should return 'value'

# Check expiry buffer
SNOWFLAKE_TOKEN_EXPIRY_BUFFER=60 # Not 3600!
```

## FAQ

### Q: Do I need to change my existing code?

**A**: No, the migration is automatic. Token management is transparent.

### Q: What cache driver should I use?

**A**: Redis is recommended for production. Memcached is acceptable. Avoid file cache.

### Q: Will this work with Laravel Horizon/Queues?

**A**: Yes, tokens are shared across all processes including queue workers.

### Q: What happens if Redis goes down?

**A**: The system falls back to non-atomic token generation. Performance degrades but system continues working.

### Q: Can I use different expiry buffers for different accounts?

**A**: Currently no, expiry buffer is per-provider. You can create multiple providers with different buffers.

### Q: How do I test this locally?

**A**: Use Redis locally (Docker: `docker run -d -p 6379:6379 redis`), or use database cache for testing.

### Q: Does this work with serverless (Laravel Vapor)?

**A**: Yes, but ensure you use Redis cache (not file). Vapor includes Redis by default.

### Q: What's the performance impact?

**A**: Positive! Cache hits are faster (0.1ms vs 50ms). Cache misses are similar. High concurrency is much faster (99% reduction).

### Q: Is this secure?

**A**: Yes, read SECURITY.md for full threat model and mitigations.

## Support

For issues or questions:
- GitHub Issues: https://github.com/nzscripps/laravel-snowflake-api-driver/issues
- Documentation: https://github.com/nzscripps/laravel-snowflake-api-driver
- Security: See SECURITY.md
