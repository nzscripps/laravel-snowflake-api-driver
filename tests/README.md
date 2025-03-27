# Laravel Snowflake API Driver Tests

This directory contains the test suite for the Laravel Snowflake API Driver.

## Running Tests

### Unit Tests

Unit tests can be run without any Snowflake credentials:

```bash
composer test:unit
# or
vendor/bin/phpunit --testsuite Unit
```

### Integration Tests

Integration tests require a Snowflake account with proper credentials. Follow these steps to run integration tests:

1. Copy `.env.testing` to `.env.testing.local` (this file should be excluded from git)
2. Fill in your Snowflake test credentials in the `.env.testing.local` file:

```
SNOWFLAKE_TEST_URL=your-account.snowflakecomputing.com
SNOWFLAKE_TEST_ACCOUNT=your-account
SNOWFLAKE_TEST_USER=your-username
SNOWFLAKE_TEST_PUBLIC_KEY=your-public-key-fingerprint
SNOWFLAKE_TEST_PRIVATE_KEY=your-complete-private-key-content
SNOWFLAKE_TEST_PASSPHRASE=your-private-key-passphrase
SNOWFLAKE_TEST_WAREHOUSE=your-warehouse
SNOWFLAKE_TEST_DATABASE=your-test-database
SNOWFLAKE_TEST_SCHEMA=your-test-schema
```

3. Run the integration tests:

```bash
# Using Composer (recommended)
composer test:integration

# Or directly with the script
php run-integration-tests.php
```

Both methods will properly load the environment variables from `.env.testing.local` before running the tests.

**Important Notes**:
- Tests will create a temporary table called `SNOWFLAKE_API_TEST_TABLE` in your configured schema
- Make sure your credentials have permissions to create/drop tables in the specified schema
- The tests will clean up the temporary table when finished
- Avoid using production schemas for testing to prevent any accidental data loss
- Snowflake object names are typically uppercase by default (e.g., `TEST_TABLE` not `test_table`)
- The SnowflakeService `ExecuteQuery` method only supports one SQL statement at a time

## Test Structure

- `Unit/`: Unit tests for components in isolation
- `Integration/`: Integration tests for Snowflake API interactions
- `fixtures/`: Test data and SQL files
- `TestCase.php`: Base test case class
- `TestDataManager.php`: Helper trait for managing test data

## Adding New Tests

When adding new tests:

1. Place unit tests in the appropriate subdirectory of `Unit/`
2. Place integration tests in `Integration/`
3. Use the `TestCase` base class for all tests
4. Mock all external dependencies in unit tests
5. For integration tests, use the `TestDataManager` trait if you need test data

## Debugging

If tests are failing, you can enable verbose output:

```bash
vendor/bin/phpunit --testsuite Unit --verbose
```

Or debug a specific test:

```bash
vendor/bin/phpunit --filter it_converts_data_types_correctly
```

### Common Issues

1. **Multiple SQL statements**: The Snowflake API driver only supports executing one SQL statement at a time. Split multiple statements into separate `ExecuteQuery` calls.

2. **Case sensitivity**: Snowflake object names are typically uppercase by default. Use uppercase names in your queries (e.g., `SELECT * FROM TEST_TABLE`).

3. **Schema permissions**: Ensure your configured user has permissions to create and drop tables in the specified schema.

4. **Authentication errors**: Verify that your private key and passphrase are correctly formatted. Remember that newlines in private keys must be preserved.

5. **Table name conflicts**: If your schema already contains tables that might conflict with test tables, use the `getTestTableName()` method in `TestDataManager` to customize the test table name. By default, the integration tests use `SNOWFLAKE_API_TEST_TABLE` to avoid conflicts with common table names. 