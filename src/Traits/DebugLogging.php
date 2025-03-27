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
     *
     * @param string $message
     * @param array $context
     * @return void
     */
    protected function debugLog(string $message, array $context = []): void
    {
        if ($this->isDebugEnabled()) {
            Log::debug($message, $context);
        }
    }
    
    /**
     * Check if debug logging is enabled
     * 
     * Checks the SNOWFLAKE_DEBUG_LOGGING environment variable.
     * Only returns true if the value is explicitly set to 'true'.
     *
     * @return bool
     */
    private function isDebugEnabled(): bool
    {
        if ($this->isDebugEnabled === null) {
            // Check for environment variable and only return true if it's strictly 'true'
            $envValue = getenv('SNOWFLAKE_DEBUG_LOGGING');
            $this->isDebugEnabled = ($envValue === 'true');
            
            // Alternate check using env() helper if available
            if (!$this->isDebugEnabled && function_exists('env')) {
                $envValue = env('SNOWFLAKE_DEBUG_LOGGING');
                $this->isDebugEnabled = ($envValue === true || $envValue === 'true');
            }
        }
        
        return $this->isDebugEnabled;
    }
} 