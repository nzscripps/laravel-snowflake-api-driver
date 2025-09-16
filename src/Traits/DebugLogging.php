<?php

namespace LaravelSnowflakeApi\Traits;

use Illuminate\Support\Facades\Log;

trait DebugLogging
{
    /**
     * Flag to indicate if debug logging is enabled
     *
     * @var bool|null
     */
    private $isDebugEnabled = null;

    /**
     * Log debug information if SNOWFLAKE_DEBUG_LOGGING is enabled
     */
    protected function debugLog(string $message, array $context = []): void
    {
        if ($this->isDebugEnabled()) {
            // Process context to avoid circular references
            $safeContext = $this->sanitizeContextForLogging($context);
            Log::debug($message, $safeContext);
        }
    }

    /**
     * Check if debug logging is enabled
     *
     * Checks the SNOWFLAKE_DEBUG_LOGGING environment variable.
     * Only returns true if the value is explicitly set to 'true'.
     */
    private function isDebugEnabled(): bool
    {
        if ($this->isDebugEnabled === null) {
            // Check for environment variable and only return true if it's strictly 'true'
            $envValue = getenv('SNOWFLAKE_DEBUG_LOGGING');
            $this->isDebugEnabled = ($envValue === 'true');

            // Alternate check using env() helper if available
            if (! $this->isDebugEnabled && function_exists('env')) {
                $envValue = env('SNOWFLAKE_DEBUG_LOGGING');
                $this->isDebugEnabled = ($envValue === true || $envValue === 'true');
            }
        }

        return $this->isDebugEnabled;
    }

    /**
     * Sanitize context data to prevent circular references
     */
    private function sanitizeContextForLogging(array $context): array
    {
        $safe = [];

        foreach ($context as $key => $value) {
            if (is_object($value)) {
                // For complex objects, just log the class name
                if (method_exists($value, '__toString')) {
                    $safe[$key] = (string) $value;
                } else {
                    $safe[$key] = get_class($value).' instance';
                }
            } elseif (is_array($value)) {
                // Recursively process nested arrays
                $safe[$key] = $this->sanitizeContextForLogging($value);
            } elseif (is_resource($value)) {
                // For resources, log the resource type
                $safe[$key] = get_resource_type($value).' resource';
            } else {
                // Primitives can be logged as-is
                $safe[$key] = $value;
            }
        }

        return $safe;
    }
}
