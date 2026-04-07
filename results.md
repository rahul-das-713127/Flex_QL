# FlexQL Performance Results

## 10,000,000 Row Insertion Benchmark
- **Target Rows**: 10,000,000
- **Batch Size**: 1 (Single-row INSERTs)
- **Elapsed Time**: 9151 ms
- **Throughput**: 1,092,776 rows/sec

## Unit Test Results
- **Total Tests**: 21
- **Passed**: 21
- **Failed**: 0

### Verified Features:
- CREATE TABLE with schema enforcement
- Single and multi-row INSERT
- SELECT * and column-specific queries
- WHERE clause with comparison operators
- INNER JOIN logic
- Primary Key index rebuild and usage
- Expiration filtering (read-time)
- Malformed SQL handling
