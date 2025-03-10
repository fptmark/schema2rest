#!/usr/bin/env python
import sys
import os
from pathlib import Path
from jinja2 import Environment, FileSystemLoader

# Add parent directory to path to allow importing helpers
sys.path.append(str(Path(__file__).parent.parent))
from common import Schema

def get_jinja_env() -> Environment:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    template_dir = os.path.join(script_dir, "templates", "routes")

    env = Environment(
        loader=FileSystemLoader(template_dir),
        autoescape=False,
        trim_blocks=True,
        lstrip_blocks=True
    )
    # Optionally add built-ins here if needed
    return env

def generate_routes(schema_file: str, path_root: str):
    print("Generating routes...")
    schema = Schema(schema_file)
    env = get_jinja_env()
    route_template = env.get_template('route.j2')

    routes_dir = os.path.join(path_root, 'app', 'routes')
    os.makedirs(routes_dir, exist_ok=True)

    for entity_name, entity_def in schema.concrete_entities().items():

        rendered = route_template.render(entity=entity_name)
        out_filename = f"{entity_name.lower()}_router.py"
        out_path = os.path.join(routes_dir, out_filename)
        with open(out_path, 'w') as f:
            f.write(rendered)
        print(f"Generated {out_filename}")
    print("Route generation complete!")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python gen_routes.py <schema.yaml> <path_root>")
        sys.exit(1)
    schema_file = sys.argv[1]
    path_root = sys.argv[2]
    generate_routes(schema_file, path_root)
