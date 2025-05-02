#!/usr/bin/env python3
"""
Code generation script - Generates models, routes, etc. from schema
"""
import sys
from pathlib import Path
from src.common.helpers import valid_backend

from src.generators.models.gen_model_main import generate_models
from src.generators.gen_routes import generate_routes
from src.generators.gen_service_routes import generate_service_routes
from src.generators.gen_db import generate_db
from src.generators.gen_main import generate_main
from src.convert.schemaConvert import convert_schema

def generate_code(schema_file, output_dir, backend):
    """
    Main entry point for code generation
    """
        
    try:
        yaml = convert_schema(schema_file)
        if yaml:
            generate_main(yaml, output_dir)
            generate_routes(yaml, output_dir)
            generate_db(yaml, output_dir, backend)
            generate_models(yaml, output_dir, backend)
            generate_service_routes(yaml, output_dir)
            
            print("Code generation completed successfully!")
            return 0
        else:
            print("Error: Schema conversion failed.")
            return 1

    except Exception as e:
        print(f"Error during code generation: {e}")
        import traceback
        traceback.print_exc()
        return 1
    return 0

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python generate_code.py <schema.mmd> <output_path> [<backend>]")
        sys.exit(1)

    backend = sys.argv[3] if len(sys.argv) >= 3 else "mongo"
    if not valid_backend(backend):
        print(f"Invalid backend: {backend}. Supported backends are: mongo, postgres.")
        sys.exit(1)

    results = generate_code(sys.argv[1], sys.argv[2], backend)