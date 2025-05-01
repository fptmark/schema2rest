import sys
from pathlib import Path
from typing import List

from common.helpers import write, valid_backend
from common import Schema
from common import Templates


# Paths
BASE_DIR = Path(__file__).resolve().parent
RESERVED_TYPES = {"ISODate", "ObjectId"}  # Reserved types to skip

def generate_db(schema_path, path_root, backend):
    """
    Generate the db.py file for MongoDB connection and Beanie initialization.
    """
    # Load the YAML schema
    schema = Schema(schema_path)

    templates = Templates(BASE_DIR, "db", backend)

    # Dynamically add imports for each model
    models = ""
    db_imports: List[str] = []
    for model, _ in schema.concrete_entities().items():
            model_lower = model.lower()
            db_imports.append(f"from app.models.{model_lower}_model import {model}")
            models += f"{model}, "

    rendered = templates.render("1", {"db_imports": db_imports, "models": models})

    write(path_root, "", "db.py", rendered)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python gen_db.py <schema.yaml> <path_root> [<backend>]")
        sys.exit(1)
    
    schema_file = sys.argv[1]
    path_root = sys.argv[2]
    backend = sys.argv[3] if len(sys.argv) > 3 else "mongo"

    if valid_backend(backend):
        generate_db(schema_file, path_root, backend)
