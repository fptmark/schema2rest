# Schema Conversion Tests

This directory contains tests for the schema conversion functionality.

## Test Files

- `test_schema_convert.py`: Simple test script that demonstrates the current implementation
- `basic_tests/`: Contains test schema and output files
  - `test_schema.mmd`: A sample schema file for testing
  - `test_output.yaml`: Example output from previous runs
  - `output.yaml`: Generated output from running the test script

## Running Tests

To run the basic conversion test:

```bash
# From the tests directory
./test_schema_convert.py

# From the project root
python3 convert/tests/test_schema_convert.py
```

## Old Implementation

The previous, modular implementation of the schema conversion functionality is preserved in the `old_implementation/` directory for reference purposes only. It is not used in the current codebase.