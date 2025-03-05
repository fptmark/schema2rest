#!/usr/bin/env python3
"""
Code generation script - Generates models, routes, etc. from schema
"""
import sys
import os
from pathlib import Path

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from generators import (
    generate_models, 
    generate_routes, 
    generate_db, 
    generate_main, 
    generate_service_routes,
)

def generate(schema_file, path_root):
    """
    Main entry point for code generation
    """
        
    try:
        print("Generating models...")
        generate_models(schema_file, path_root)
        
        print("Generating routes...")
        generate_routes(schema_file, path_root)
        
        print("Generating service routes...")
        generate_service_routes(schema_file, path_root)
        
        print("Generating database module...")
        generate_db(schema_file, path_root)
        
        print("Generating main application file...")
        generate_main(schema_file, path_root)
        
        print("Code generation completed successfully!")
        return 0
    except Exception as e:
        print(f"Error during code generation: {e}")
        import traceback
        traceback.print_exc()
        return 1
    return 0

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python generate_code.py <schema.json> <output_dir>")
        sys.exit(1)
    sys.exit(generate(sys.argv[1], sys.argv[2]))