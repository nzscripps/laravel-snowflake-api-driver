<?php

declare(strict_types=1);

namespace Tests\Unit\Services;

use Exception;
use Illuminate\Support\Facades\Cache;
use LaravelSnowflakeApi\Services\SnowflakeService;
use Mockery;
use ReflectionMethod;
use Tests\TestCase;

/**
 * Test response parsing in SnowflakeService
 *
 * These tests verify:
 * - gzdecode handles valid gzip data correctly
 * - gzdecode fallback works when native function fails
 * - Control character cleaning removes problematic chars
 * - Control character cleaning preserves valid JSON whitespace
 * - JSON parsing works after control character removal
 * - Large responses are handled without memory issues
 */
class SnowflakeServiceResponseParsingTest extends TestCase
{
    protected function setUp(): void
    {
        parent::setUp();

        // Mock Cache facade to avoid needing actual cache drivers
        Cache::shouldReceive('get')->andReturn(null)->byDefault();
        Cache::shouldReceive('put')->andReturn(true)->byDefault();
        Cache::shouldReceive('lock')->andReturnSelf()->byDefault();
        Cache::shouldReceive('block')->andReturn(null)->byDefault();
    }

    protected function tearDown(): void
    {
        Mockery::close();
        parent::tearDown();
    }

    // ============================================
    // gzdecode Tests
    // ============================================

    public function test_gzdecode_handles_valid_gzip_data(): void
    {
        $service = $this->createService();
        $original = '{"data": "test value with special chars: àéîõü"}';
        $compressed = gzencode($original);

        $result = $this->callGzdecode($service, $compressed);

        $this->assertEquals($original, $result);
    }

    public function test_gzdecode_handles_empty_json_object(): void
    {
        $service = $this->createService();
        $original = '{}';
        $compressed = gzencode($original);

        $result = $this->callGzdecode($service, $compressed);

        $this->assertEquals($original, $result);
    }

    public function test_gzdecode_handles_large_data(): void
    {
        $service = $this->createService();
        // Create ~1MB of JSON data
        $data = [];
        for ($i = 0; $i < 10000; $i++) {
            $data[] = ['id' => $i, 'name' => "Item $i with some padding text to make it larger"];
        }
        $original = json_encode(['data' => $data]);
        $compressed = gzencode($original);

        $result = $this->callGzdecode($service, $compressed);

        $this->assertEquals($original, $result);
    }

    public function test_gzdecode_handles_data_with_unicode(): void
    {
        $service = $this->createService();
        $original = '{"data": "日本語テスト 中文测试 🎉"}';
        $compressed = gzencode($original);

        $result = $this->callGzdecode($service, $compressed);

        $this->assertEquals($original, $result);
    }

    public function test_gzdecode_handles_data_with_newlines(): void
    {
        $service = $this->createService();
        $original = "{\n  \"data\": \"value\",\n  \"other\": \"value2\"\n}";
        $compressed = gzencode($original);

        $result = $this->callGzdecode($service, $compressed);

        $this->assertEquals($original, $result);
    }

    public function test_gzdecode_throws_on_invalid_gzip_data(): void
    {
        $service = $this->createService();
        $invalidData = 'this is not gzip data';

        $this->expectException(Exception::class);
        $this->callGzdecode($service, $invalidData);
    }

    public function test_gzdecode_handles_truncated_gzip_data(): void
    {
        $service = $this->createService();
        $original = '{"data": "test"}';
        $compressed = gzencode($original);
        // Truncate the compressed data severely (first 5 bytes only - just the gzip header)
        $truncated = substr($compressed, 0, 5);

        // Truncated gzip data should either throw an exception or return partial/empty result
        // depending on how the native gzdecode and fallback handle it
        try {
            $result = $this->callGzdecode($service, $truncated);
            // If it doesn't throw, the result should be different from original
            $this->assertNotEquals($original, $result);
        } catch (Exception $e) {
            // Expected - truncated data should fail
            $this->assertStringContainsString('decompress', strtolower($e->getMessage()));
        }
    }

    // ============================================
    // Control Character Removal Tests
    // ============================================

    public function test_removeControlChars_removes_null_bytes(): void
    {
        $service = $this->createService();
        $input = "test\x00value";

        $result = $this->callRemoveControlChars($service, $input);

        $this->assertEquals('testvalue', $result);
    }

    public function test_removeControlChars_removes_bell_character(): void
    {
        $service = $this->createService();
        $input = "test\x07value";

        $result = $this->callRemoveControlChars($service, $input);

        $this->assertEquals('testvalue', $result);
    }

    public function test_removeControlChars_removes_backspace(): void
    {
        $service = $this->createService();
        $input = "test\x08value";

        $result = $this->callRemoveControlChars($service, $input);

        $this->assertEquals('testvalue', $result);
    }

    public function test_removeControlChars_preserves_tab(): void
    {
        $service = $this->createService();
        $input = "test\tvalue";

        $result = $this->callRemoveControlChars($service, $input);

        $this->assertEquals("test\tvalue", $result);
    }

    public function test_removeControlChars_preserves_newline(): void
    {
        $service = $this->createService();
        $input = "test\nvalue";

        $result = $this->callRemoveControlChars($service, $input);

        $this->assertEquals("test\nvalue", $result);
    }

    public function test_removeControlChars_preserves_carriage_return(): void
    {
        $service = $this->createService();
        $input = "test\rvalue";

        $result = $this->callRemoveControlChars($service, $input);

        $this->assertEquals("test\rvalue", $result);
    }

    public function test_removeControlChars_removes_vertical_tab(): void
    {
        $service = $this->createService();
        $input = "test\x0Bvalue";

        $result = $this->callRemoveControlChars($service, $input);

        $this->assertEquals('testvalue', $result);
    }

    public function test_removeControlChars_removes_form_feed(): void
    {
        $service = $this->createService();
        $input = "test\x0Cvalue";

        $result = $this->callRemoveControlChars($service, $input);

        $this->assertEquals('testvalue', $result);
    }

    public function test_removeControlChars_removes_escape_character(): void
    {
        $service = $this->createService();
        $input = "test\x1Bvalue";

        $result = $this->callRemoveControlChars($service, $input);

        $this->assertEquals('testvalue', $result);
    }

    public function test_removeControlChars_removes_delete_character(): void
    {
        $service = $this->createService();
        $input = "test\x7Fvalue";

        $result = $this->callRemoveControlChars($service, $input);

        $this->assertEquals('testvalue', $result);
    }

    public function test_removeControlChars_handles_multiple_control_chars(): void
    {
        $service = $this->createService();
        $input = "test\x00\x01\x02\x03\x04value";

        $result = $this->callRemoveControlChars($service, $input);

        $this->assertEquals('testvalue', $result);
    }

    public function test_removeControlChars_handles_string_without_control_chars(): void
    {
        $service = $this->createService();
        $input = 'test value with no control chars';

        $result = $this->callRemoveControlChars($service, $input);

        $this->assertEquals($input, $result);
    }

    public function test_removeControlChars_handles_empty_string(): void
    {
        $service = $this->createService();

        $result = $this->callRemoveControlChars($service, '');

        $this->assertEquals('', $result);
    }

    public function test_removeControlChars_preserves_valid_json_structure(): void
    {
        $service = $this->createService();
        $input = "{\n\t\"data\": \"value\",\r\n\t\"other\": \"value2\"\n}";

        $result = $this->callRemoveControlChars($service, $input);

        // Should be valid JSON after cleaning
        $decoded = json_decode($result, true);
        $this->assertNotNull($decoded);
        $this->assertEquals('value', $decoded['data']);
    }

    public function test_removeControlChars_handles_control_chars_in_json_values(): void
    {
        $service = $this->createService();
        // Simulate a JSON response where a string value contains a control char
        $input = '{"data": "value with' . "\x00" . 'null byte"}';

        $result = $this->callRemoveControlChars($service, $input);

        $this->assertEquals('{"data": "value withnull byte"}', $result);
        // Should be valid JSON after cleaning
        $decoded = json_decode($result, true);
        $this->assertNotNull($decoded);
    }

    public function test_removeControlChars_handles_large_string(): void
    {
        $service = $this->createService();
        // Create a large string (1MB) with occasional control characters
        $input = '';
        for ($i = 0; $i < 100000; $i++) {
            $input .= 'normal text ';
            if ($i % 1000 === 0) {
                $input .= "\x00"; // Add null byte every 1000 iterations
            }
        }

        $result = $this->callRemoveControlChars($service, $input);

        // Should not contain null bytes
        $this->assertStringNotContainsString("\x00", $result);
        // Should still contain the normal text
        $this->assertStringContainsString('normal text', $result);
    }

    // ============================================
    // Integration: gzdecode + control char removal + JSON parsing
    // ============================================

    public function test_full_pipeline_with_clean_data(): void
    {
        $service = $this->createService();
        $originalData = ['data' => [['id' => 1, 'name' => 'Test']]];
        $json = json_encode($originalData);
        $compressed = gzencode($json);

        // Decompress
        $decompressed = $this->callGzdecode($service, $compressed);
        // Remove control chars
        $cleaned = $this->callRemoveControlChars($service, $decompressed);
        // Parse JSON
        $result = json_decode($cleaned, true);

        $this->assertEquals($originalData, $result);
    }

    public function test_full_pipeline_with_control_chars_in_data(): void
    {
        $service = $this->createService();
        // Create JSON with embedded control character
        $json = '{"data": [{"id": 1, "name": "Test' . "\x00" . 'Value"}]}';
        $compressed = gzencode($json);

        // Decompress
        $decompressed = $this->callGzdecode($service, $compressed);
        // Remove control chars
        $cleaned = $this->callRemoveControlChars($service, $decompressed);
        // Parse JSON
        $result = json_decode($cleaned, true);

        $this->assertNotNull($result);
        $this->assertEquals('TestValue', $result['data'][0]['name']);
    }

    // ============================================
    // Transient Error Detection Tests
    // ============================================

    public function test_isTransientError_detects_http_500(): void
    {
        $service = $this->createService();

        $result = $this->callIsTransientError($service, 'HTTP/2 500 returned for URL', 500);

        $this->assertTrue($result);
    }

    public function test_isTransientError_detects_http_502(): void
    {
        $service = $this->createService();

        $result = $this->callIsTransientError($service, 'Bad Gateway', 502);

        $this->assertTrue($result);
    }

    public function test_isTransientError_detects_http_503(): void
    {
        $service = $this->createService();

        $result = $this->callIsTransientError($service, 'Service Unavailable', 503);

        $this->assertTrue($result);
    }

    public function test_isTransientError_detects_connection_reset(): void
    {
        $service = $this->createService();

        $result = $this->callIsTransientError($service, 'Connection reset by peer', 0);

        $this->assertTrue($result);
    }

    public function test_isTransientError_detects_timeout(): void
    {
        $service = $this->createService();

        $result = $this->callIsTransientError($service, 'Operation timed out', 0);

        $this->assertTrue($result);
    }

    public function test_isTransientError_returns_false_for_400(): void
    {
        $service = $this->createService();

        $result = $this->callIsTransientError($service, 'Bad Request', 400);

        $this->assertFalse($result);
    }

    public function test_isTransientError_returns_false_for_401(): void
    {
        $service = $this->createService();

        $result = $this->callIsTransientError($service, 'Unauthorized', 401);

        $this->assertFalse($result);
    }

    public function test_isTransientError_returns_false_for_404(): void
    {
        $service = $this->createService();

        $result = $this->callIsTransientError($service, 'Not Found', 404);

        $this->assertFalse($result);
    }

    // ============================================
    // Helper Methods
    // ============================================

    private function createService(): SnowflakeService
    {
        return new SnowflakeService(
            'https://test.snowflakecomputing.com',
            'test_account',
            'test_user',
            'test_public_key_fingerprint',
            $this->getTestPrivateKey(),
            '',
            'test_warehouse',
            'test_database',
            'test_schema',
            30
        );
    }

    private function getTestPrivateKey(): string
    {
        return <<<'EOD'
-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEAy8Dbv8prQbENkqwPQEqHNdCjb3TYJ6F+FvRHVQKRjLLG5VUE
tcLVW7dVjYs2y9D7dGHkKFwjlFhMHTSZsKTFXhEyZJRPQiECDGYxHgxBqFxYLbhN
yrJKqGBZsI4WPvqrZH1KMO6bGH1TKhcwB1YZ8PuNSQg8wTKpVILwWghJKlDkQwCj
P8N0S+gPSYVqQJYB7bSZGPSxQjCcEI0KjQr7hqQXHMCUwjCqKjrYeNkb0y0kNYLh
jDqbJ9KHxNMKfqFB7PJ9xN3ljRoC5MnKqHqPkDQDmqXqNQMGWV8gTkgLJLxQNzYQ
QclZN5FVlPXKIvJL7LrFLLYQGFYPQ3T1kMXCsQIDAQABAoIBAFxXKAcTXyQSNXPG
o8KUxN8bR7xbMkhYFjLSRMBP3qB1qkYXWa5QQFEFZfqcHsrDNf9Z9fCZXsxZPqPb
hDEqHLXzFHLEKYNJCRqhOsK5+k3l0kfQN6kU8AvVsjvLgxWWQK7T8KQvRQGGrGxI
tPKLVqQlKCDKbYxMUE3uJE8L8hQaQwGhGJMqXClqc7qLlFKLm6fmqHW8TQaXvxHy
4tN3F0VfQCxQU8ELmVrHbNvNGNEfJ0RpEqMLwBCqLlRxJMKdH8JQKqLLbKCQIVJM
CdXKJHLaZ6vOGWYQ2kBYQqVJLi7bG6A7xjQhNFBJLPvHZxZjLKpqDqRqYgHQ7sRx
M5QZwgECgYEA8RlLqFMhUCLKqL0j8qSQNjBjIX7qPAoJPgF7kTmCWx9eKqLdQFxw
GKYVqZpEOFVJTtKDT5rPSZJPmAKQPqWQYVfL+RvFKZGKH4wPxHXpxzWZYLKQGBYO
HjSAoRR0r8XpWRgvVCEwmLHkqkQAFRdMQyQhHLLWc8pUqJ2CqLhYZmECgYEA2Axj
QMXmBslVBqLhJVAjn9HKqw1P3vYqW3nQQUEKXBHLEYEKKHLZCbhHPXqLRPSLGFWX
PdQPHVQHHWQVRxHNJKYkKb6BdPFXRMJdLrLXLfZCJQVNlQJWQJWQFbmVQGKXHGLg
KzBKTJQKQWQXQLgLNHGHLfHKQJLLKQPQQLfQQLkCgYBjQJqPQKQqLJQPQJqPQKqL
JQPQJqPQKQqLJQPQJqPQKQqLJQPQJqPQKQqLJQPQJqPQKQqLJQPQJqPQKQqLJQPQ
JqPQKQqLJQPQJqPQKQqLJQPQJqPQKQqLJQPQJqPQKQqLJQPQJqPQKQqLJQPQJqPQ
KQqLJQPQJqPQKQqLJQPQJqPQKQqLJQPQJqPQKQqLJQECgYA0lULHLQKQXHPXqKQL
fQPLLHKQPfQVQHLHKQPfQVQHLHKQPfQVQHLHKQPfQVQHLHKQPfQVQHLHKQPfQVQH
LHKQPfQVQHLHKQPfQVQHLHKQPfQVQHLHKQPfQVQHLHKQPfQVQHLHKQPfQVQHLHKQ
PfQVQHLHKQPfQVQHLHKQPfQVQHLHKQPfQVQHLHKQPfQVQQKBgH0KQLHfQKQLPXQL
HfQKQLPXQLHfQKQLPXQLHfQKQLPXQLHfQKQLPXQLHfQKQLPXQLHfQKQLPXQLHfQKQ
LPXQLHfQKQLPXQLHfQKQLPXQLHfQKQLPXQLHfQKQLPXQLHfQKQLPXQLHfQKQLPXQL
HfQKQLPXQLHfQKQLPXQLHfQKQLPXQLHfQKQLPXQLHfQKQLP==
-----END RSA PRIVATE KEY-----
EOD;
    }

    private function callGzdecode(SnowflakeService $service, string $data): string
    {
        $reflection = new ReflectionMethod(SnowflakeService::class, 'gzdecode');
        $reflection->setAccessible(true);

        return $reflection->invoke($service, $data);
    }

    private function callRemoveControlChars(SnowflakeService $service, string $content): string
    {
        $reflection = new ReflectionMethod(SnowflakeService::class, 'removeControlChars');
        $reflection->setAccessible(true);

        return $reflection->invoke($service, $content);
    }

    private function callIsTransientError(SnowflakeService $service, string $message, int $code): bool
    {
        $reflection = new ReflectionMethod(SnowflakeService::class, 'isTransientError');
        $reflection->setAccessible(true);

        return $reflection->invoke($service, $message, $code);
    }
}
