import sys
from pathlib import Path

# Add parent directory to path to allow importing helpers
sys.path.append(str(Path(__file__).parent.parent))
from common import helpers
from common import Schema

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

    # Import routes dynamically for valid entities and determine the list of services for each entity
    services = {}
    for entity_name, details in schema.concrete_entities().items():
        entity_lower = entity_name.lower()
        lines.append(f"from app.routes.{entity_lower}_router import router as {entity_lower}_router\n")
        for inherits in details.get("inherits", []):
            if isinstance(inherits, dict) and "service" in inherits:
                for service_instance in inherits["service"]:
                    words = service_instance.split('.')
                    services.setdefault(entity_name, []).append(words[0])

    # Add routing for each entity-service pair
    lines.extend("\n#Add routing for each entity-service pair\n")
    for entity_name in services.keys():
        for service in services[entity_name]:
            entity_lower = entity_name.lower()
            entity_service = f"{entity_lower}_{service}"
            lines.append(f"from routes.services.{service}.{entity_service}_routes import router as {entity_service}_router\n")

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
    for entity_name, details in schema.concrete_entities().items():
        entity_lower = entity_name.lower()
        lines.append(f"app.include_router({entity_lower}_router, prefix='/{entity_lower}', tags=['{entity_name}'])\n")
        # Add routing for each entity-service pair
        for inherits in details.get("inherits", []):
            if isinstance(inherits, dict) and "service" in inherits:
                for service_instance in inherits["service"]:
                    words = service_instance.split('.')
                    lines.append(f"app.include_router({entity_lower}_{words[0]}_router, prefix='/api/{entity_lower}/{words[0]}', tags=['{entity_name}'])\n")

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
