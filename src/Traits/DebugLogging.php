<?php

namespace LaravelSnowflakeApi\Traits;

use Illuminate\Support\Facades\Log;

trait DebugLogging
{
    /**
     * Log debug information if SF_DEBUG is enabled
     *
     * @param string $message
     * @param array $context
     * @return void
     */
    protected function debugLog(string $message, array $context = []): void
    {
        if (env('SF_DEBUG', true)) {
            Log::info($message, $context);
        }
    }
} 