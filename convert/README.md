# Schema Conversion Module

This directory contains the code for converting MMD schema files to YAML format. The implementation consists of a single file `schemaConvert.py`, which provides a direct, robust approach to parsing MMD files.

## Structure

- `schemaConvert.py`: The complete implementation for MMD to YAML conversion
- `__init__.py`: Exports the `convert_schema` function
- `tests/`: Contains test files and old implementation for reference

## Implementation Details

The implementation uses a class-based approach with the `SchemaParser` class to handle MMD file parsing. It extracts:

- Entities with their fields
- Validation attributes from the `@validate` decorator
- Service definitions from the `@service` decorator
- Inheritance information from the `@inherit` decorator
- Unique constraints from the `@unique` decorator
- Dictionaries from the `@dictionary` decorator

## Usage

```python
from convert import convert_schema

# Convert a schema file
convert_schema('path/to/schema.mmd', 'path/to/output.yaml')
```

## Testing

Test files are located in the `tests/` directory. The old, modular implementation has been preserved in `tests/old_implementation/` for reference.