#!/usr/bin/env python3
"""
Basic test script for schemaConvert.py
"""
import sys
import os
from pathlib import Path

# Add parent directory to path to allow importing from convert module
sys.path.append(str(Path(__file__).parent.parent.parent))
from convert import convert_schema

def test_conversion():
    """
    Test basic schema conversion functionality
    """
    # Get the path to test schema file
    test_dir = Path(__file__).parent
    test_schema = test_dir / "basic_tests" / "test_schema.mmd"
    test_output = test_dir / "basic_tests" / "output.json"
    
    # Convert the schema
    print(f"Converting {test_schema} to {test_output}...")
    
    # This is for debugging only
    from convert.schemaConvert import SchemaParser
    lines = []
    with open(test_schema, "r") as f:
        lines = [line.rstrip() for line in f]
    
    parser = SchemaParser()
    entities, relationships, dictionaries = parser.parse_mmd(lines)
    
    # Show the results
    print("\nParsed entities:")
    for entity_name, entity in entities.items():
        print(f"Entity: {entity_name}")
        print(f"  Fields: {list(entity.get('fields', {}).keys())}")
        print(f"  Inherits: {entity.get('inherits', [])}")
    
    success = convert_schema(str(test_schema), str(test_output))
    
    # Report result
    if success:
        print("Conversion successful!")
        print(f"Output written to {test_output}")
    else:
        print("Conversion failed!")
    
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(test_conversion())