#!/usr/bin/env python
import sys
import os
from pathlib import Path
from jinja2 import Environment, FileSystemLoader

from common.helpers import write
from common import Schema

BASE_DIR = Path(__file__).resolve().parent

def get_jinja_env() -> Environment:
    template_dir = os.path.join(BASE_DIR, "templates", "routes")

    env = Environment(
        loader=FileSystemLoader(template_dir),
        autoescape=False,
        trim_blocks=True,
        lstrip_blocks=True
    )
    # Optionally add built-ins here if needed
    return env

def generate_routes(schema_file: str, path_root: str, backend: str):

    print("Generating routes...")
    schema = Schema(schema_file)
    env = get_jinja_env()
    route_template = env.get_template('route.j2')

    for entity_name, entity_def in schema.concrete_entities().items():

        rendered = route_template.render(entity=entity_name)
        write(path_root, backend, "routes", f"{entity_name.lower()}_router.py", rendered)

    print("Route generation complete!")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(f"Usage: python {sys.argv[0]} <schema.yaml> <output_path> [<backend>]")
        sys.exit(1)
    
    schema_file = sys.argv[1]
    path_root = sys.argv[2]
    backend = sys.argv[3] if len(sys.argv) >= 3 else "mongo"

    generate_routes(schema_file, path_root, backend)
