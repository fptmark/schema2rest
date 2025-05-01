#!/usr/bin/env python3
import sys
import os
from pathlib import Path
from jinja2 import Environment, FileSystemLoader

from common.helpers import write, valid_backend
from common import Schema  # Your Schema class (in schema.py) should accept (schema_file, path_root)
from common import helpers

############################
# JINJA ENVIRONMENT SETUP
############################
def combine_filter(dict1, dict2):
    new_dict = dict1.copy()
    new_dict.update(dict2)
    return new_dict

def get_jinja_env() -> Environment:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    template_dir = os.path.join(script_dir, "templates", "main")
    env = Environment(
        loader=FileSystemLoader(template_dir),
        autoescape=False,
        trim_blocks=True,
        lstrip_blocks=True,
        extensions=['jinja2.ext.do']
    )
    env.filters['combine'] = combine_filter
    env.filters['split'] = lambda s, sep=None: s.split(sep)
    return env

def generate_main(schema_file, path_root, backend):

    print("Generating main...")
    schema = Schema(schema_file)
    env = get_jinja_env()
    
    try:
        model_template = env.get_template("main.j2")
    except Exception as e:
        print("Error loading main.j2 template:", e)
        return

    rendered = model_template.render(
        entities=schema.concrete_entities(),  # Pass the concrete entities
        services=schema.services(),           # Pass the _services mapping from the YAML
        backend=backend,
    )
        
    helpers.write(path_root, backend, "", "main.py", rendered)

if __name__ == "__main__":
    # Original CLI handling: expect two positional arguments.
    if len(sys.argv) < 2:
        print("Usage: python gen_main.py <schema.yaml> <path_root> [<backend>]")
        sys.exit(1)
    
    schema_file = sys.argv[1]
    path_root = sys.argv[2]
    backend = sys.argv[3] if len(sys.argv) > 3 else "mongo"

    if helpers.valid_backend(backend):
        generate_main(schema_file, path_root, backend)
