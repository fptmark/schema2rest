import sys
import os
from pathlib import Path

# Add the parent directory to the path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from convert.schemaConvert import convert_schema
from generate_code import generate as generate_code
from generators.update_indicies import update_indexes

def main():
    """
    Main entry point for schema2rest - handles CLI arguments
    """
    if len(sys.argv) < 2:
        print("Usage: python schema2rest.py <schema.mmd>")
        return 1

    mmd_file = sys.argv[1]
    # Use the second argument as path_root if provided, otherwise use current directory
    path_root = sys.argv[2] if len(sys.argv) > 2 else "."

    # convert the schema to a format that can be used by the generators (xxx.json)
    convert_schema(mmd_file, path_root)

    # use the json to create the code and update indexes for uniques 
    schema_file = mmd_file.replace(".mmd", ".json")
    generate_code(schema_file, path_root)
    update_indexes(schema_file)

    return 1

if __name__ == "__main__":
    sys.exit(main())