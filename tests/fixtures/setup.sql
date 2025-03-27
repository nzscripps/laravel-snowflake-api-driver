-- Create test schema
CREATE SCHEMA IF NOT EXISTS test_schema;

-- Create test table
CREATE OR REPLACE TABLE test_schema.test_table (
    id INTEGER,
    string_col VARCHAR,
    date_col DATE,
    bool_col BOOLEAN
);

-- Insert test data
INSERT INTO test_schema.test_table 
VALUES 
    (1, 'test1', '2023-01-01', true),
    (2, 'test2', '2023-01-02', false); 