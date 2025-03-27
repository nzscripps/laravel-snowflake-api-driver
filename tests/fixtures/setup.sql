-- Use the existing schema (doesn't try to create a new one)
-- No CREATE SCHEMA statement needed

-- Create test table with a unique name to avoid conflicts
CREATE OR REPLACE TABLE SNOWFLAKE_API_TEST_TABLE (
    id INTEGER,
    string_col VARCHAR,
    date_col DATE,
    bool_col BOOLEAN
);

-- Insert test data
INSERT INTO SNOWFLAKE_API_TEST_TABLE 
VALUES 
    (1, 'test1', '2023-01-01', true),
    (2, 'test2', '2023-01-02', false); 