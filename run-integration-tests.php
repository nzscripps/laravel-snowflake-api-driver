<?php
/**
 * Laravel Snowflake API Driver - Integration Test Runner
 * 
 * This script:
 * 1. Loads environment variables from .env.testing.local
 * 2. Runs the integration test suite with those variables
 * 
 * Usage: php run-integration-tests.php
 */

// Load environment variables from .env.testing.local
if (file_exists(__DIR__ . '/.env.testing.local')) {
    $envFile = file_get_contents(__DIR__ . '/.env.testing.local');
    $lines = explode("\n", $envFile);
    
    foreach ($lines as $line) {
        $line = trim($line);
        
        // Skip empty lines and comments
        if (empty($line) || strpos($line, '#') === 0) {
            continue;
        }
        
        // Parse key=value pairs
        if (strpos($line, '=') !== false) {
            list($key, $value) = explode('=', $line, 2);
            $key = trim($key);
            $value = trim($value);
            
            // Remove surrounding quotes if present
            if ((substr($value, 0, 1) === '"' && substr($value, -1) === '"') || 
                (substr($value, 0, 1) === "'" && substr($value, -1) === "'")) {
                $value = substr($value, 1, -1);
            }
            
            // Set as environment variable
            putenv("$key=$value");
        }
    }
    
    echo "Environment loaded from .env.testing.local\n";
} else {
    echo "Warning: .env.testing.local file not found. Integration tests may fail.\n";
    echo "Please copy .env.testing.local.example to .env.testing.local and fill in your credentials.\n";
}

// Run the integration tests
$result = passthru('vendor/bin/phpunit --testsuite Integration');
exit($result); 