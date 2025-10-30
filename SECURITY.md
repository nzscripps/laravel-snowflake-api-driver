# Security Architecture

This document describes the security architecture of the Laravel Snowflake API Driver, with a focus on thread-safe token management.

## Overview

The driver implements secure, atomic token management to prevent common security vulnerabilities in distributed systems:

- **Thundering Herd Prevention**: Only one process generates tokens when expired
- **Race Condition Prevention**: Atomic operations using distributed locks
- **Token Leakage Prevention**: Secure handling of sensitive credentials
- **Token Staleness Prevention**: Hierarchical caching with expiry buffers

## Thread-Safe Token Management

### Architecture

The `ThreadSafeTokenProvider` class implements a multi-layered security approach:

```
Request → Static Cache → Laravel Cache → Atomic Lock → JWT Generation
   ↓           ↓              ↓              ↓              ↓
0.1ms       0.5ms          1-5ms         50-200ms      Full Process
```

### Security Features

#### 1. Atomic Token Generation

**Problem**: Without atomic operations, 100 concurrent requests might all generate tokens simultaneously.

**Solution**: Distributed locks ensure only ONE process generates a token at a time:

```php
// Pseudocode
if (cache_miss) {
    lock = acquire_lock(timeout=5s)
    if (lock.acquired) {
        // Double-check cache (another process might have generated it)
        if (still_cache_miss) {
            token = generate_jwt()
            cache.put(token)
        }
        lock.release()
    }
}
```

**Implementation**: Uses Laravel's `Cache::lock()` with Redis/Memcached backend.

#### 2. Double-Checked Locking

**Problem**: While waiting for a lock, another process might generate the token.

**Solution**: Check cache again AFTER acquiring lock:

```php
lock.block(timeout, function() {
    // CRITICAL: Double-check cache
    $existingToken = cache.get(key);
    if ($existingToken !== null) {
        return $existingToken; // Use existing
    }
    return generate_new_token(); // Generate only if needed
});
```

**Benefit**: Prevents unnecessary token generation under high concurrency.

#### 3. Token Expiry Buffer

**Problem**: Token might expire mid-request, causing API call to fail.

**Solution**: Consider tokens expired N seconds before actual expiry:

```php
$expiryBuffer = 60; // seconds
if (time() >= $token_expiry - $expiryBuffer) {
    // Token too close to expiry, generate new one
}
```

**Configuration**: Default 60s, configurable via constructor:

```php
$provider = new ThreadSafeTokenProvider($config, $expiryBuffer = 120);
```

#### 4. Graceful Degradation

**Problem**: If locking fails (e.g., cache driver doesn't support locks), system should continue working.

**Solution**: Fallback to direct generation:

```php
try {
    $lock = Cache::lock($key, $timeout);
    return $lock->block($timeout, $callback);
} catch (LockTimeoutException $e) {
    // Timeout - check cache one more time
    $existing = cache.get();
    if ($existing) return $existing;

    // Last resort: generate without lock
    return generate_token();
}
```

**Impact**: System degrades gracefully instead of failing completely.

## Private Key Security

### Storage

Private keys are stored in environment variables and loaded at runtime:

```bash
SNOWFLAKE_PRIVATE_KEY="-----BEGIN RSA PRIVATE KEY-----\n..."
SNOWFLAKE_PRIVATE_KEY_PASSPHRASE="your-passphrase"
```

### Memory Protection

1. **No Persistence**: Private keys are never written to disk or logs
2. **Scoped Lifetime**: Keys are loaded only when generating tokens
3. **Redacted Logging**: Debug logs redact sensitive values

### Recommendations

1. Use encrypted private keys with strong passphrases
2. Rotate keys regularly (every 90 days)
3. Store keys in secure secret managers (AWS Secrets Manager, HashiCorp Vault)
4. Use IAM-based key access controls
5. Monitor key usage and set up alerts for anomalies

## Cache Driver Requirements

### Production-Ready Drivers

| Driver | Atomic Locks | Distributed | Production | Notes |
|--------|--------------|-------------|------------|-------|
| **Redis** | Yes (SETNX) | Yes | Recommended | Best performance, full lock support |
| **Memcached** | Yes (CAS) | Yes | Acceptable | Good performance, lock support |
| **Database** | Depends | Yes | Acceptable | Slower, lock support varies by DB |

### Not Recommended

| Driver | Issue | Risk |
|--------|-------|------|
| **File** | No distributed locks | Race conditions in multi-server deployments |
| **Array** | In-memory only | Lost on every request, testing only |
| **Null** | No caching | Performance degradation |

### Configuration Validation

The driver automatically validates cache configuration at boot:

```php
// In SnowflakeApiServiceProvider
private function validateCacheDriver(): void {
    $driver = config('cache.default');

    if (in_array($driver, ['file', 'array', 'null'])) {
        Log::warning('Cache driver does not support atomic locks', [
            'current_driver' => $driver,
            'recommended_drivers' => ['redis', 'memcached'],
        ]);
    }
}
```

## Threat Model & Mitigations

### Threat: Token Leakage

**Attack**: Attacker gains access to cached tokens via cache inspection.

**Mitigation**:
- Tokens stored with 60-second expiry buffer
- Short-lived tokens (1 hour lifetime)
- Secure cache backends (Redis with AUTH, encrypted connections)

**Residual Risk**: If cache backend is compromised, tokens are still valid until expiry.

**Recommendation**: Use Redis ACLs to restrict cache access.

### Threat: Token Replay

**Attack**: Attacker captures token and uses it for unauthorized API calls.

**Mitigation**:
- Short token lifetime (1 hour)
- Snowflake's JWT validation (issuer, subject, expiry)
- HTTPS for all API communications

**Residual Risk**: Token valid for up to 1 hour after capture.

**Recommendation**: Monitor for suspicious API usage patterns.

### Threat: Private Key Extraction

**Attack**: Attacker gains access to server memory and extracts private key.

**Mitigation**:
- Keys loaded only during token generation
- Keys not persisted in long-lived objects
- Environment variables with restricted access

**Residual Risk**: If attacker gains root access to server, keys can be extracted.

**Recommendation**: Use hardware security modules (HSMs) for key storage in highly sensitive environments.

### Threat: Thundering Herd

**Attack**: Coordinated requests exhaust server resources by triggering mass token generation.

**Mitigation**:
- Atomic locking ensures only one token generation at a time
- Hierarchical caching reduces load
- Lock timeouts prevent indefinite blocking

**Residual Risk**: If cache backend fails, all requests fall back to generation.

**Recommendation**: Monitor cache health and set up failover.

### Threat: Clock Skew

**Attack**: Server clock drift causes token expiry mismatch between servers.

**Mitigation**:
- 60-second expiry buffer accommodates minor clock skew
- NTP synchronization recommended

**Residual Risk**: Severe clock skew (>60s) can cause token validation failures.

**Recommendation**: Use NTP with monitoring and alerts.

### Threat: Cache Poisoning

**Attack**: Attacker injects invalid token into cache.

**Mitigation**:
- Cache keys include account/user identifiers
- Tokens validated on use by Snowflake API
- Cache backend authentication required

**Residual Risk**: If cache backend is compromised, invalid tokens can be injected.

**Recommendation**: Use Redis AUTH and encrypted connections (TLS).

## Security Best Practices

### Development

1. **Never commit private keys** to version control
2. **Use separate keys** for dev/staging/production
3. **Rotate keys** when developers leave the team
4. **Enable debug logging** only in non-production environments

### Deployment

1. **Use Redis or Memcached** for cache in production
2. **Enable Redis AUTH** and TLS encryption
3. **Restrict cache access** to application servers only
4. **Monitor token generation** rate for anomalies
5. **Set up alerts** for cache failures

### Operations

1. **Rotate keys** every 90 days
2. **Monitor token expiry** rates
3. **Track cache hit rates** (should be >95%)
4. **Review logs** for failed token generations
5. **Test failover** scenarios regularly

## Compliance Considerations

### SOC 2

- **Audit Logging**: Enable debug logging in production (when SNOWFLAKE_DEBUG_LOGGING=true)
- **Access Controls**: Restrict private key access to authorized personnel
- **Encryption**: Use TLS for all API communications
- **Monitoring**: Track token generation and usage patterns

### GDPR

- **Data Minimization**: Tokens contain minimal PII (user identifier only)
- **Right to Erasure**: Clear token cache when user is deleted
- **Data Retention**: Tokens auto-expire after 1 hour

### PCI DSS

- **Key Management**: Rotate keys regularly, use strong passphrases
- **Access Control**: Restrict cache backend access
- **Logging**: Log all authentication attempts
- **Monitoring**: Alert on unusual token generation patterns

## Incident Response

### Compromised Private Key

1. **Immediately** rotate the private key in Snowflake
2. **Clear all token caches** across all servers
3. **Update** environment variables with new key
4. **Restart** application servers to load new key
5. **Review** logs for unauthorized access
6. **Notify** security team and affected users

### Compromised Token

1. **Identify** token expiry time (max 1 hour impact)
2. **Monitor** Snowflake audit logs for suspicious queries
3. **Consider** rotating private key if multiple tokens compromised
4. **Review** cache access logs for unauthorized access
5. **Update** security controls to prevent recurrence

### Cache Backend Failure

1. **Monitor** application logs for lock timeout errors
2. **Verify** fallback to direct token generation is working
3. **Restore** cache backend as priority
4. **Review** performance impact of cache failure
5. **Update** runbooks based on lessons learned

## Security Auditing

### Audit Checklist

- [ ] Private keys stored securely (not in code)
- [ ] Private keys encrypted with strong passphrases
- [ ] Redis/Memcached used for production cache
- [ ] Cache backend has authentication enabled
- [ ] TLS enabled for cache connections
- [ ] Debug logging disabled in production
- [ ] Token generation rate monitored
- [ ] Cache hit rate monitored (>95%)
- [ ] Key rotation schedule defined (90 days)
- [ ] Incident response plan documented
- [ ] Security team has access to logs

### Monitoring Metrics

```
Token Generation Rate:
- Target: <1 per minute (with good cache hit rate)
- Alert: >10 per minute (possible cache failure)

Cache Hit Rate:
- Target: >95% (most requests use cached tokens)
- Alert: <80% (cache ineffective or failing)

Lock Timeout Rate:
- Target: 0% (no lock contention)
- Alert: >1% (high concurrency or slow generation)

Token Expiry Rate:
- Target: ~1 per hour per account (natural expiry)
- Alert: >10 per hour (possible time sync issues)
```

## Contact

For security issues or questions, contact:
- Security Team: security@example.com
- On-call: pagerduty@example.com

For vulnerability reports, use:
- GitHub Security Advisory: https://github.com/your-org/repo/security/advisories
