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
     * Log debug information if debug logging is enabled
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
     * This method checks configuration in this order:
     * 1. Local property if already set
     * 2. Laravel config 'snowflake.debug_logging'
     * 3. Laravel config 'app.debug'
     * 4. Environment variable 'SF_DEBUG'
     * 5. Default to false
     *
     * @return bool
     */
    private function isDebugEnabled(): bool
    {
        if ($this->isDebugEnabled === null) {
            $this->isDebugEnabled = false;
            
            if (function_exists('config')) {
                $this->isDebugEnabled = config('snowflake.debug_logging', false) || config('app.debug', false);
            } else if (function_exists('env')) {
                $this->isDebugEnabled = env('SF_DEBUG', false);
            }
        }
        
        return $this->isDebugEnabled;
    }
} 