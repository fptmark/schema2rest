#!/usr/bin/env python3
"""
Code generation script - Generates models, routes, etc. from schema
"""
import sys
from pathlib import Path
# from common.helpers import valid_backend

from generators.models.gen_model_main import generate_models
from generators.gen_routes import generate_routes
from generators.gen_service_routes import generate_service_routes
from generators.gen_db import generate_db
from generators.gen_main import generate_main
from convert.schemaConvert import convert_schema
from tools.mongo.update_indicies import update_indexes

def generate_code(schema_file, base_output_dir, project_name, config_file=None):
    """
    Main entry point for code generation
    """
        
    try:
        yaml = convert_schema(schema_file)
        if yaml:
            generate_main(yaml, base_output_dir, project_name)
            generate_routes(yaml, base_output_dir)
            # generate_db(yaml, base_output_dir)
            generate_models(yaml, base_output_dir)
            generate_service_routes(yaml, base_output_dir)
            if config_file:
                update_indexes(yaml, config_file)
            else:
                print("No config file provided, skipping index update.")
            
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
    if len(sys.argv) < 4:
        print("Usage: python generate_code.py <schema.mmd> <base_output_path> <project name> [<config_file>]")
        sys.exit(1)

    optional_param_start = 4

    config_file = None if len(sys.argv) <= optional_param_start + 1 else sys.argv[optional_param_start + 1]
    results = generate_code(sys.argv[1], sys.argv[2], sys.argv[3], config_file)