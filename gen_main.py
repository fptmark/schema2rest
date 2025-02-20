from pathlib import Path
import sys
import helpers
from schema import Schema

script_dir = Path(__file__).resolve().parent

# Paths
MAIN_FILE = Path("app/main.py")
RESERVED_TYPES = {"ISODate", "ObjectId"}  # Reserved types to skip
# TEMPLATE = "generators/templates/main/main"
TEMPLATE = str(script_dir / "templates" / "main" / "main")

def generate_main(schema_path, path_root):
    lines: list[str] = []

    schema = Schema(schema_path)

    # add imports for all services
    services = schema.services()
    for service in services:
        words = service.split('.')
        alias = words[0].capitalize()
        imported = words[1].capitalize() + alias
        lines.append(f"from app.services.{service} import {imported} as {alias}\n")

    # Start building the main.py content
    lines.extend(helpers.read_file_to_array(TEMPLATE, 1))

    # Import routes dynamically for valid entities
    for entity, _ in schema.concrete_entities().items():
        entity_lower = entity.lower()
        lines.append(f"from app.routes.{entity_lower}_router import router as {entity_lower}_router\n")

    # Initialize FastAPI app
    lines.extend( helpers.read_file_to_array(TEMPLATE, 2))

    # Add service initializers
    lines.append("# Add service initializers\n")
    for service in schema.services():
        words = service.split('.')
        alias = words[0].capitalize()
        lines.append(f"    print(f'>>> Initializing service {service}')\n")
        lines.append(f"    await {alias}.initialize(config[\'{service}\'])\n")
    lines.append("\n")

    # Register routes dynamically
    lines.append("# Register routes\n")
    for entity, _ in schema.concrete_entities().items():
        entity_lower = entity.lower()
        lines.append(f"app.include_router({entity_lower}_router, prefix='/{entity_lower}', tags=['{entity}'])\n")

    # Add root endpoint
    lines.extend( helpers.read_file_to_array(TEMPLATE, 3))

    # Save main.py
    outfile = helpers.generate_file(path_root, MAIN_FILE, lines)
    print(f">>> Generated {outfile}")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python gen_main.py <schema.yaml> <path_root")
        sys.exit(1)

    schema_file = sys.argv[1]
    path_root = sys.argv[2]
    generate_main(schema_file, path_root)
