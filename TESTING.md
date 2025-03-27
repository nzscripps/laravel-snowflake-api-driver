# Laravel Snowflake API Driver - Testing

This document describes the testing infrastructure for the Laravel Snowflake API Driver.

## Test Structure

The testing infrastructure is organized into the following components:

- **Unit Tests**: Test individual components in isolation
- **Integration Tests**: Test integration with Snowflake API
- **Fixtures**: Test data and configurations

### Directory Structure

```
tests/
├── fixtures/             # Test fixtures and data
├── Integration/          # Integration tests 
├── Unit/                 # Unit tests
│   ├── Services/         # Tests for service classes
│   └── Traits/           # Tests for traits
├── TestCase.php          # Base test case
└── TestDataManager.php   # Helper trait for test data
```

## Running Tests

### Prerequisites

- PHP 7.4 or later
- Composer

### Configuration

For integration tests, you need to set up a Snowflake account and configure the `.env.testing` file with your credentials:

```env
SNOWFLAKE_TEST_URL=https://your-account.snowflakecomputing.com
SNOWFLAKE_TEST_ACCOUNT=your-account
SNOWFLAKE_TEST_USER=your-username
SNOWFLAKE_TEST_PUBLIC_KEY=your-public-key-fingerprint
SNOWFLAKE_TEST_PRIVATE_KEY=your-private-key-content
SNOWFLAKE_TEST_PASSPHRASE=your-private-key-passphrase
SNOWFLAKE_TEST_WAREHOUSE=your-warehouse
SNOWFLAKE_TEST_DATABASE=your-database
SNOWFLAKE_TEST_SCHEMA=your-schema
```

### Running Tests

Use the following Composer scripts to run tests:

```bash
# Run all tests
composer test

# Run only unit tests
composer test:unit

# Run only integration tests
composer test:integration
```

## Writing Tests

### Unit Tests

Unit tests should focus on testing individual components in isolation, using mocks for dependencies:

```php
class ResultTest extends TestCase
{
    /** @test */
    public function it_converts_data_types_correctly()
    {
        $result = new Result();
        $result->setFields([
            ['name' => 'bool_col', 'type' => 'BOOLEAN']
        ]);
        
        $result->setData([['true']]);
        
        $converted = $result->toArray();
        
        $this->assertIsBool($converted[0]['bool_col']);
    }
}
```

### Integration Tests

Integration tests should test actual interactions with the Snowflake API:

```php
class SnowflakeServiceIntegrationTest extends TestCase
{
    use TestDataManager;
    
    /** @test */
    public function it_executes_simple_query()
    {
        $result = $this->service->ExecuteQuery('SELECT 1 as TEST');
        
        $this->assertCount(1, $result);
        $this->assertEquals(1, $result->first()['TEST']);
    }
}
```

## Improvements and Optimizations

During testing implementation, we made several improvements to the codebase:

1. **Enhanced `DebugLogging` trait**:
   - Added hierarchical configuration checking
   - Implemented caching of debug status for performance
   - Standardized on Log::debug level for consistency

2. **Optimized HTTP Client usage**:
   - Improved handling of concurrent requests
   - Enhanced error handling and retry mechanisms

3. **Result Processing Improvements**:
   - Optimized data type conversion for better performance
   - Added proper handling of different date/time formats

## Continuous Integration

A GitHub Actions workflow is included to run tests automatically on push and pull requests:

```yaml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    
    strategy:
      matrix:
        php: [7.4, 8.0, 8.1]
        laravel: [8.*, 9.*, 10.*]
```

Integration tests are only run on the main branch with proper credentials configured as GitHub secrets. 