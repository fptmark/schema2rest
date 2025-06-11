#!/usr/bin/env python
import sys
import os
from pathlib import Path
from typing import List
import common.template as template
from common.helpers import write
from common import Schema

BASE_DIR = Path(__file__).resolve().parent

def generate_routes(schema_file: str, path_root: str):

    print("Generating routes...")
    schema = Schema(schema_file)

    templates = template.Templates(BASE_DIR, "routes")

    for entity, defs in schema.concrete_entities().items():
        if defs.get("abstract", False):
            continue

        vars_map = {
            'entity': entity,
            'entity_lower': entity.lower(),
        }
        rendered = templates.render(str(1), vars_map)
        write(path_root, "routes", f"{entity.lower()}_router.py", rendered)
        # print(f"Generated {entity.lower()}_model.py")

    print("Route generation complete!")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Usage: python {sys.argv[0]} <schema.yaml> <output_path>")
        sys.exit(1)
    
    schema_file = sys.argv[1]
    path_root = sys.argv[2]

    generate_routes(schema_file, path_root)
