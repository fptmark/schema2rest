import sys
from pathlib import Path

# Add parent directory to path to allow importing helpers
sys.path.append(str(Path(__file__).parent.parent))
from common import helpers
from common import Schema

script_dir = Path(__file__).resolve().parent

# Paths
DB_FILE = Path("app/db.py")
RESERVED_TYPES = {"ISODate", "ObjectId"}  # Reserved types to skip
TEMPLATE = str(script_dir / "templates" / "db" / "db.txt")

def generate_db(schema_path, path_root):
    """
    Generate the db.py file for MongoDB connection and Beanie initialization.
    """
    # Load the YAML schema
    schema = Schema(schema_path)

    # Generate db.py
    db_lines = [
        "from motor.motor_asyncio import AsyncIOMotorClient\n",
        "from beanie import init_beanie\n",
        "import logging\n",
        "\n",
    ]

    # Dynamically add imports for each model
    models = ""
    for model, _ in schema.concrete_entities().items():
            model_lower = model.lower()
            db_lines.append(f"from app.models.{model_lower}_model import {model}\n")
            models += f"{model}, "
            # models += f"{model.capitalize()}, "

    template = helpers.read_file_to_array(TEMPLATE)
    template = [ line.replace("{models}", models[:-1]) for line in template]

    db_lines.extend(template)

    # Ensure the directory exists
    outfile = helpers.generate_file(path_root, DB_FILE, db_lines)
    print(f">>> Generated {outfile}")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python gen_db.py <schema.yaml> <path_root")
        sys.exit(1)

    schema_file = sys.argv[1]
    path_root = sys.argv[2]
    generate_db(schema_file, path_root)
