<?php

declare(strict_types=1);

namespace Tests\Unit\Services;

use Exception;
use Illuminate\Contracts\Cache\Lock;
use Illuminate\Contracts\Cache\LockTimeoutException;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\Log;
use LaravelSnowflakeApi\Exceptions\SnowflakeApiException;
use LaravelSnowflakeApi\Services\SnowflakeConfig;
use LaravelSnowflakeApi\Services\ThreadSafeTokenProvider;
use Mockery;
use Tests\TestCase;

class ThreadSafeTokenProviderTest extends TestCase
{
    private SnowflakeConfig $config;

    private $cacheStore;

    protected function setUp(): void
    {
        parent::setUp();

        Log::shouldReceive('warning')->byDefault();
        Log::shouldReceive('error')->byDefault();
        $this->cacheStore = Mockery::mock();
        Cache::shouldReceive('store')->byDefault()->withAnyArgs()->andReturn($this->cacheStore);
        $this->cacheStore->shouldReceive('lock')->byDefault()->withAnyArgs()->andThrow(new Exception('Lock not supported'));

        // Create mock config with test credentials
        $this->resetProviderState();
        $this->config = $this->makeConfig();
    }

    protected function tearDown(): void
    {
        Mockery::close();
        parent::tearDown();
    }

    public function test_get_token_from_static_cache()
    {
        // This test verifies that static cache is checked first
        $provider = new ThreadSafeTokenProvider($this->config);

        // Mock Laravel cache to ensure it's not called
        $this->cacheStore->shouldReceive('get')->never();
        $this->cacheStore->shouldReceive('lock')->never();

        // Pre-populate static cache using reflection
        $reflection = new \ReflectionClass($provider);
        $staticCache = $reflection->getProperty('staticCache');
        $staticCache->setAccessible(true);

        $staticKey = 'test_account:test_user';
        $testToken = 'static_cached_token.test.signature';
        $staticCache->setValue(null, [
            $staticKey => [
                'token' => $testToken,
                'expiry' => time() + 3600, // Valid for 1 hour
            ],
        ]);

        // Get token - should return static cached value
        $token = $provider->getToken();

        $this->assertEquals($testToken, $token);
    }

    public function test_get_token_from_laravel_cache()
    {
        // This test verifies Laravel cache is checked when static cache misses
        $provider = new ThreadSafeTokenProvider($this->config);

        $cacheKey = 'snowflake_api_token:test_account:test_user';
        $testToken = 'laravel_cached_token.test.signature';
        $cachedData = [
            'token' => $testToken,
            'expiry' => time() + 3600,
        ];

        // Mock cache to return token
        $this->cacheStore->shouldReceive('get')
            ->twice()
            ->with($cacheKey)
            ->andReturn($cachedData);

        // Get token - should return Laravel cached value
        $token = $provider->getToken();

        $this->assertEquals($testToken, $token);
    }

    public function test_uses_configured_cache_store_instead_of_global_default()
    {
        $cacheKey = 'snowflake_api_token:test_account:test_user';
        $testToken = 'redis_cached_token.test.signature';

        Cache::shouldReceive('store')
            ->byDefault()
            ->with('redis')
            ->andReturn($this->cacheStore);
        Cache::shouldReceive('store')
            ->never()
            ->with('file');
        $provider = new ThreadSafeTokenProvider($this->makeConfig(cacheDriver: 'redis'));

        $this->cacheStore->shouldReceive('get')
            ->twice()
            ->with($cacheKey)
            ->andReturn([
                'token' => $testToken,
                'expiry' => time() + 3600,
            ]);

        $token = $provider->getToken();

        $this->assertEquals($testToken, $token);
    }

    public function test_get_token_generates_new_when_cache_misses()
    {
        // This test verifies new token generation when all caches miss
        $provider = new ThreadSafeTokenProvider($this->config);
        $this->setDriverLockSupport('array', true);

        $cacheKey = 'snowflake_api_token:test_account:test_user';
        $lockKey = 'snowflake_api_token_lock:test_account:test_user';

        // Mock cache miss
        $this->cacheStore->shouldReceive('get')
            ->once()
            ->with($cacheKey)
            ->andReturn(null);

        // Mock lock acquisition (simulate lock not supported)
        $mockLock = Mockery::mock(\Illuminate\Contracts\Cache\Lock::class);
        $this->cacheStore->shouldReceive('lock')
            ->once()
            ->with($lockKey, 5)
            ->andReturn($mockLock);

        $mockLock->shouldReceive('block')
            ->once()
            ->andThrow(new Exception('Lock not supported'));

        // Mock cache operations for storing new token
        $this->cacheStore->shouldReceive('get')
            ->with($cacheKey)
            ->andReturn(null);

        $this->cacheStore->shouldReceive('put')
            ->once()
            ->withArgs(function ($key, $data, $duration) use ($cacheKey) {
                return $key === $cacheKey
                    && is_array($data)
                    && isset($data['token'])
                    && isset($data['expiry'])
                    && $duration > 0;
            });

        // Get token - should generate new one
        $token = $provider->getToken();

        $this->assertNotEmpty($token);
        $this->assertStringContainsString('.', $token); // JWT format
    }

    public function test_lock_timeout_falls_back_to_generation()
    {
        // This test verifies graceful fallback when lock times out
        $provider = new ThreadSafeTokenProvider($this->config);
        $this->setDriverLockSupport('array', true);

        $cacheKey = 'snowflake_api_token:test_account:test_user';
        $lockKey = 'snowflake_api_token_lock:test_account:test_user';

        // Mock cache miss
        $this->cacheStore->shouldReceive('get')
            ->twice()
            ->with($cacheKey)
            ->andReturn(null);

        // Mock lock timeout
        $mockLock = Mockery::mock(\Illuminate\Contracts\Cache\Lock::class);
        $this->cacheStore->shouldReceive('lock')
            ->once()
            ->with($lockKey, 5)
            ->andReturn($mockLock);

        $mockLock->shouldReceive('block')
            ->once()
            ->andThrow(new LockTimeoutException);

        // Mock cache operations for fallback generation
        $this->cacheStore->shouldReceive('put')
            ->once()
            ->withArgs(function ($key, $data, $duration) use ($cacheKey) {
                return $key === $cacheKey
                    && is_array($data)
                    && isset($data['token']);
            });

        // Get token - should fall back to generation
        $token = $provider->getToken();

        $this->assertNotEmpty($token);
    }

    public function test_double_checked_locking_prevents_race_condition()
    {
        // This test verifies that double-checked locking works correctly
        $provider = new ThreadSafeTokenProvider($this->config);
        $this->setDriverLockSupport('array', true);

        $cacheKey = 'snowflake_api_token:test_account:test_user';
        $lockKey = 'snowflake_api_token_lock:test_account:test_user';
        $existingToken = 'existing_token.from.another_process';
        $cachedData = [
            'token' => $existingToken,
            'expiry' => time() + 3600,
        ];

        $this->cacheStore->shouldReceive('get')
            ->times(3)
            ->with($cacheKey)
            ->andReturn(null, $cachedData, $cachedData);

        // Mock successful lock acquisition
        $mockLock = Mockery::mock(\Illuminate\Contracts\Cache\Lock::class);
        $this->cacheStore->shouldReceive('lock')
            ->once()
            ->with($lockKey, 5)
            ->andReturn($mockLock);

        // Simulate another process generated token while we waited for lock
        $mockLock->shouldReceive('block')
            ->once()
            ->with(5, Mockery::type('callable'))
            ->andReturnUsing(fn ($timeout, $callback) => $callback());

        // Get token - should return existing token, not generate new one
        $token = $provider->getToken();

        $this->assertEquals($existingToken, $token);
    }

    public function test_expiry_buffer_prevents_expired_tokens()
    {
        // This test verifies expiry buffer prevents using nearly-expired tokens
        $expiryBuffer = 120; // 2 minutes
        $provider = new ThreadSafeTokenProvider($this->config, $expiryBuffer);

        $cacheKey = 'snowflake_api_token:test_account:test_user';

        // Create token that expires in 1 minute (within buffer)
        $almostExpiredToken = 'almost_expired_token.test.signature';
        $cachedData = [
            'token' => $almostExpiredToken,
            'expiry' => time() + 60, // Expires in 1 minute
        ];

        // Mock cache returning almost-expired token
        $this->cacheStore->shouldReceive('get')
            ->with($cacheKey)
            ->andReturn($cachedData);

        // Mock token generation since cached token should be rejected
        $this->cacheStore->shouldReceive('forget')
            ->once()
            ->with($cacheKey);

        $mockLock = Mockery::mock(\Illuminate\Contracts\Cache\Lock::class);
        $this->cacheStore->shouldReceive('lock')
            ->andReturn($mockLock);

        $mockLock->shouldReceive('block')
            ->andThrow(new Exception('Lock not supported'));

        $this->cacheStore->shouldReceive('get')
            ->with($cacheKey)
            ->andReturn(null);

        $this->cacheStore->shouldReceive('put')
            ->once();

        // Get token - should generate new one, not use almost-expired
        $token = $provider->getToken();

        $this->assertNotEmpty($token);
        $this->assertNotEquals($almostExpiredToken, $token);
    }

    public function test_clear_token_cache_removes_all_caches()
    {
        // This test verifies clearTokenCache removes from all cache levels
        $provider = new ThreadSafeTokenProvider($this->config);

        $cacheKey = 'snowflake_api_token:test_account:test_user';

        // Pre-populate static cache
        $reflection = new \ReflectionClass($provider);
        $staticCache = $reflection->getProperty('staticCache');
        $staticCache->setAccessible(true);
        $staticCache->setValue(null, [
            'test_account:test_user' => [
                'token' => 'test_token',
                'expiry' => time() + 3600,
            ],
        ]);

        // Mock Laravel cache forget
        $this->cacheStore->shouldReceive('forget')
            ->once()
            ->with($cacheKey);

        // Clear cache
        $provider->clearTokenCache();

        // Verify static cache is empty
        $staticCacheValue = $staticCache->getValue();
        $this->assertArrayNotHasKey('test_account:test_user', $staticCacheValue);
    }

    public function test_validates_cache_drivers_independently_per_store()
    {
        $redisLock = Mockery::mock(Lock::class);
        $memcachedLock = Mockery::mock(Lock::class);
        $redisStore = Mockery::mock();
        $memcachedStore = Mockery::mock();

        Cache::shouldReceive('store')
            ->once()
            ->with('redis')
            ->andReturn($redisStore);
        $redisStore->shouldReceive('lock')
            ->once()
            ->withArgs(fn ($key, $seconds) => str_starts_with($key, 'snowflake_test_lock_') && $seconds === 1)
            ->andReturn($redisLock);
        $redisLock->shouldReceive('forceRelease')->once();

        $providerRedis = new ThreadSafeTokenProvider($this->makeConfig(cacheDriver: 'redis'));
        $this->assertInstanceOf(ThreadSafeTokenProvider::class, $providerRedis);

        Cache::shouldReceive('store')
            ->once()
            ->with('memcached')
            ->andReturn($memcachedStore);
        $memcachedStore->shouldReceive('lock')
            ->once()
            ->withArgs(fn ($key, $seconds) => str_starts_with($key, 'snowflake_test_lock_') && $seconds === 1)
            ->andReturn($memcachedLock);
        $memcachedLock->shouldReceive('forceRelease')->once();

        $providerMemcached = new ThreadSafeTokenProvider($this->makeConfig(
            account: 'second_account',
            user: 'second_user',
            cacheDriver: 'memcached'
        ));

        $this->assertInstanceOf(ThreadSafeTokenProvider::class, $providerMemcached);
    }

    public function test_validate_expiry_buffer_enforces_minimum()
    {
        // This test verifies expiry buffer validation
        $provider = new ThreadSafeTokenProvider($this->config);

        $this->expectException(SnowflakeApiException::class);
        $this->expectExceptionMessage('Token expiry buffer must be at least 30 seconds');

        $reflection = new \ReflectionClass($provider);
        $method = $reflection->getMethod('validateExpiryBuffer');
        $method->setAccessible(true);
        $method->invoke($provider, 10); // Too low
    }

    public function test_validate_expiry_buffer_enforces_maximum()
    {
        // This test verifies expiry buffer validation
        $provider = new ThreadSafeTokenProvider($this->config);

        $this->expectException(SnowflakeApiException::class);
        $this->expectExceptionMessage('Token expiry buffer cannot exceed 600 seconds');

        $reflection = new \ReflectionClass($provider);
        $method = $reflection->getMethod('validateExpiryBuffer');
        $method->setAccessible(true);
        $method->invoke($provider, 700); // Too high
    }

    /**
     * Get a test RSA private key for JWT generation testing
     *
     * @return string PEM-encoded private key
     */
    private function getTestPrivateKey(): string
    {
        // This is a test-only RSA private key (2048-bit)
        // DO NOT use in production
        return <<<'EOT'
-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDDQA5zw+PlPEGy
va47NyvlcK8jwqO6Y6jIKIF5/w/nK2bb/9EQSjYwOLcAzqulz+j/Qrqane6Zvrq6
ftMnXBPtr6R+arSW8VpXhpGQnsEBimtQnzJf0ehroLVzcSwauXNhAmlRuqvT6jPA
MaaGHCvnbHBBGue/owktyESC9ucfdKpILjvvZ6a+LJpUyKbLIt114S/v/2uyptV5
L2ujyECmjB5rDQIgZsms7ztw/7rP5oHW2Xwe31cXNm32FLaF+5xh0LLnBSo+lIjs
Lutri7F+jI9PLtOdQGTCtaMHe7JXuzhTRMiqoxqN52+KLF2cAGtaKwsGhuw4Vx4O
wJexvRBFAgMBAAECggEAFwkTq2zNElqNlbzzNWFI+ITW5O0ty+u+Gf9NZ0tEYQ2p
0QLZc4aN5hqK3LmFXsaswpDo2x0O7WynMyGLX/VpFH3IdnG4ZKaM9g6WVe2k772Y
86YUl2F4flozji8n+O8wQZ27Nz1C0HmtKVuyPdhNBbyxcbzu4pklmBUsoBbpJbi1
AIQ1u8c0DFJesj/W2ovO6VWjQCH/JFrChzD0OIs8vpT4+Xdl8dEqRR4hZAuOBEPm
dl84zRkNZ+M4r/qss0tNbOwPU6/MQjb19NjNLv2burTpB5KPfrdwwp10VuP9JQxa
t4dHtYfjle4EOJuC+YE5cf2YIfBOFE4+N2zqbCrRcQKBgQDyhdOuJq5oKDOMjslV
EsDjDApJNM3vmViwbukG9pYsUM2EGPw9533fYd3c7Gm6aOM9S6g20INRsADhSY0S
7xV2R334/WKiIpMrJv0kXLc7+QjoM1I7UhtUzqCcJ/slO6B4GwSf4MU/BGzVWIwu
ZBlZJaaCoouk4uBo5ChrXSnnzwKBgQDOGbjGM9V1kBDK31daYdA/56LVlpl4tchJ
yvzHKOjMoGHHiHwPRgzvYfdUHabAtNJw1bLSNUR/NrdzZ68VJ6bz5uN4oTiVjSHO
vr8FLXZ4sIj1fs4wr5El3q+/WInSEy5933Oz0fVk8i7o6CHajsZC98LXEsiOCOeX
tdBlxaJ3qwKBgQCKUa1zS/h4y0TrS5ykeresRveu3QD/QFSG/BrHr+fiiotMZfun
StpNh0HEeMmsWZoRIY9lA/OBqYI2+34MaHOzK/86/Tk+A74wBGKoVIDlIZnk5TBn
SOLxsY+EwIDKsYFKPw6aKNYWpO11mLUK2GhkocagBxjiB8u5xzcOVOpLnQKBgEE1
FCfpeGzkstttBFc9QUUmxXcsWcw+P2tRhN6CS/2J9MXLoey3LhqC9VywsPShgT9f
7V7iqZRSPIKP2G4qCIF8mJWu9JckewDNiuRZePVAbWS2xQfUVGkV5qb0nU5Q8VGz
5AiNskVI9pyL7UIYEBRaDVQ8xiViHdv7Ez9P41JXAoGBAPBoZmeZkb2uMWf8q3vB
YtFimXSRGg3ysMhdzOQAV1f6TlRoe9SM1vzmO8c+/ctrXZxrDlQ46eMbj6puQ5aG
yqmySSC/2dw6MTELJHWzRgqKWG0scGQhdBMoYDWrM76/GdSwjP5o062tzpCE3Dml
zhJl36pyFVUPUMigejPLFZJa
-----END PRIVATE KEY-----
EOT;
    }

    private function makeConfig(
        string $account = 'test_account',
        string $user = 'test_user',
        ?string $cacheDriver = null
    ): SnowflakeConfig {
        return new SnowflakeConfig(
            'https://test.snowflakecomputing.com',
            $account,
            $user,
            'test_public_key_fingerprint',
            $this->getTestPrivateKey(),
            '',
            'test_warehouse',
            'test_database',
            'test_schema',
            30,
            $cacheDriver
        );
    }

    private function resetProviderState(): void
    {
        $reflection = new \ReflectionClass(ThreadSafeTokenProvider::class);

        foreach (['staticCache', 'driverValidated', 'driverSupportsLocks'] as $propertyName) {
            $property = $reflection->getProperty($propertyName);
            $property->setAccessible(true);
            $property->setValue(null, []);
        }
    }

    private function setDriverLockSupport(string $driver, bool $supportsLocks): void
    {
        $reflection = new \ReflectionClass(ThreadSafeTokenProvider::class);

        $validatedProperty = $reflection->getProperty('driverValidated');
        $validatedProperty->setAccessible(true);
        $validatedDrivers = $validatedProperty->getValue();
        $validatedDrivers[$driver] = true;
        $validatedProperty->setValue(null, $validatedDrivers);

        $supportsLocksProperty = $reflection->getProperty('driverSupportsLocks');
        $supportsLocksProperty->setAccessible(true);
        $driverSupportsLocks = $supportsLocksProperty->getValue();
        $driverSupportsLocks[$driver] = $supportsLocks;
        $supportsLocksProperty->setValue(null, $driverSupportsLocks);
    }
}
