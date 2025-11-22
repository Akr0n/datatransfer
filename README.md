# DataTransfer

A Python utility for migrating data between PostgreSQL databases.

## Overview

`datatrasnfer.py` is a database migration tool that transfers data from a source PostgreSQL table to a target PostgreSQL table, with support for large datasets through chunked processing and comprehensive error handling.

## Features

- **Chunked Data Transfer**: Reads and transfers data in configurable chunks (default: 500 records) to handle large tables efficiently
- **Cross-Database Support**: Can transfer data between different PostgreSQL servers
- **Transaction Management**: Commits data per chunk with automatic rollback on errors
- **Comprehensive Logging**: All operations are logged to `migrator.log` for audit trail and debugging
- **Error Handling**: Gracefully handles connection errors and insertion failures with detailed logging
- **Column Preservation**: Automatically detects and maintains column order from source table

## Requirements

- Python 3.6+
- PostgreSQL connection access to both source and target databases
- Dependencies listed in `requirements.txt`

## Installation

```bash
pip install -r requirements.txt
```

## Development & Testing

### Install Development Dependencies

```bash
pip install -r requirements-dev.txt
```

### Run Tests

```bash
# Run all tests with coverage report
pytest tests/ -v --cov=datatrasnfer --cov-report=html

# Run specific test file
pytest tests/test_datatrasnfer.py -v

# Run specific test class or method
pytest tests/test_datatrasnfer.py::TestGetConnection -v
```

### Code Quality Checks

```bash
# Run flake8 linting
flake8 datatrasnfer.py --max-line-length=120

# Run pylint
pylint datatrasnfer.py

# Format code with black
black datatrasnfer.py
```

### Test Coverage

After running tests, view the HTML coverage report:

```bash
# Generated in htmlcov/index.html
```

## Usage

1. **Configure Connection Credentials**

   Edit `datatrasnfer.py` and update the `source_conf` and `target_conf` dictionaries with your database credentials:

   ```python
   source_conf = {
       'host': 'source_host',
       'port': 5432,
       'database': 'source_db',
       'user': 'source_user',
       'password': 'source_password',
   }
   
   target_conf = {
       'host': 'target_host',
       'port': 5432,
       'database': 'target_db',
       'user': 'target_user',
       'password': 'target_password',
   }
   ```

2. **Specify Source and Target Tables**

   Update the `migrate_table()` call with your schema and table names:

   ```python
   migrate_table(
       source_conf, target_conf,
       src_schema='source_schema',
       src_table='source_table',
       tgt_schema='target_schema',
       tgt_table='target_table',
       chunk_size=500  # Adjust as needed
   )
   ```

3. **Run the Migration**

   ```bash
   python datatrasnfer.py
   ```

## Parameters

- **chunk_size**: Number of records to transfer per batch (default: 500). Reduce for memory-constrained environments, increase for better performance.

## Logging

All operations are logged to `migrator.log` with timestamps and detailed error messages. Check this file to monitor migration progress or troubleshoot issues.

## Notes

- Ensure the target table exists and has the same schema as the source table before running the migration
- The script currently continues to the next chunk on insertion errors; modify the error handling logic if you prefer different behavior
- Consider running on a test environment first to validate the migration process
