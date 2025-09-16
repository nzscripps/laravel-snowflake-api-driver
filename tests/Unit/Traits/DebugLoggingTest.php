<?php

namespace Tests\Unit\Traits;

use Illuminate\Support\Facades\Log;
use LaravelSnowflakeApi\Traits\DebugLogging;
use Mockery;
use Tests\TestCase;

class DebugLoggingTest extends TestCase
{
    protected function tearDown(): void
    {
        Mockery::close();
        parent::tearDown();
    }

    /** @test */
    public function it_logs_debug_messages()
    {
        // Mock the Log facade
        Log::shouldReceive('debug')
            ->once()
            ->with('Test message', ['key' => 'value']);

        // Create a test class that uses the trait
        $mock = new class
        {
            use DebugLogging;

            // Override to always return true for testing
            private function isDebugEnabled(): bool
            {
                return true;
            }

            public function test_log($message, $context = [])
            {
                $this->debugLog($message, $context);
            }
        };

        // Act
        $mock->testLog('Test message', ['key' => 'value']);

        // Assert is handled by the mock expectation
        $this->addToAssertionCount(1);
    }

    /** @test */
    public function it_does_not_log_when_debugging_is_disabled()
    {
        // Configure debugging to be disabled
        config(['app.debug' => false]);
        config(['snowflake.debug_logging' => false]);

        // Mock the Log facade to expect no calls
        Log::shouldReceive('debug')->never();

        // Create a class with logging disabled
        $mock = new class
        {
            use DebugLogging;

            // Override to always return false for testing
            private function isDebugEnabled(): bool
            {
                return false;
            }

            public function test_log($message, $context = [])
            {
                $this->debugLog($message, $context);
            }
        };

        // Act
        $mock->testLog('Test message', ['key' => 'value']);

        // Assert is handled by the mock expectation
        $this->addToAssertionCount(1);
    }
}
