#!/usr/bin/env python
import sys
import os
import importlib
import inspect
from jinja2 import Environment, FileSystemLoader
from schema import Schema

def get_jinja_env() -> Environment:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    template_dir = os.path.join(script_dir, "templates", "services")
    env = Environment(
        loader=FileSystemLoader(template_dir),
        autoescape=False,
        trim_blocks=True,
        lstrip_blocks=True
    )
    return env

def generate_service_routes(schema_file: str, path_root: str):
    print("Generating service routes...")
    schema = Schema(schema_file)
    env = get_jinja_env()

    entities = schema.concrete_entities()
    # Assume that the YAML _services section is separate and available via schema.services()
    # (or we can simply use the service strings defined in each entity's inherits)
    
    for entity_name, entity_def in entities.items():
        inherits = entity_def.get("inherits", [])
        # Process only service mappings in inherits
        for base in inherits:
            if isinstance(base, dict) and "service" in base:
                for service in base["service"]:
                    if not isinstance(service, str):
                        continue
                    # Compute service details from the service string (e.g., "auth.cookies.redis")
                    parts = service.split('.')
                    if len(parts) < 2:
                        continue  # not enough information
                    top_service = parts[0]    # e.g., "auth"
                    alias = top_service.capitalize()  # e.g., "Auth"
                    module_path = f"services.{top_service}.base"
                    concrete_class = f"Base{top_service.capitalize()}"
                    
                    # Dynamically import the service class
                    try:
                        mod = importlib.import_module(module_path)
                        svc_class = getattr(mod, concrete_class)
                    except Exception as e:
                        print(f"Error importing service class from {module_path}.{concrete_class}: {e}")
                        continue

                    # Inspect the service class for methods decorated with @expose_endpoint.
                    # We assume these methods have an attribute '_expose_endpoint' with metadata.
                    endpoints = []
                    for name, func in inspect.getmembers(svc_class, predicate=inspect.isfunction):
                        if hasattr(func, "_expose_endpoint"):
                            metadata = getattr(func, "_expose_endpoint")
                            metadata["route"] = f"/{entity_name.lower()}/{top_service}{metadata['route']}"
                            endpoints.append({
                                "name": f"{entity_name}{top_service.capitalize()}{name.capitalize()}",     # e.g., "UserLogin" for Auth
                                "metadata": metadata
                            })
                    if not endpoints:
                        continue

                    # Prepare output path: routes/services/<top_service>/<entity>_<top_service>_routes.py
                    out_dir = os.path.join(path_root, "routes", "services", top_service)
                    os.makedirs(out_dir, exist_ok=True)
                    out_filename = f"{entity_name.lower()}_{top_service.lower()}_routes.py"
                    out_path = os.path.join(out_dir, out_filename)
                    
                    rendered = env.get_template("service_routes.j2").render(
                        entity=entity_name,
                        service=service,
                        top_service=top_service,
                        alias=alias,
                        module_path=module_path,
                        concrete_class=concrete_class,
                        endpoints=endpoints
                    )
                    with open(out_path, "w") as f:
                        f.write(rendered)
                    print(f"Generated routes for {entity_name} using service {service} at {out_path}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python gen_service_routes.py <schema.yaml> <path_root>")
        sys.exit(1)
    schema_file = sys.argv[1]
    path_root = os.path.abspath(sys.argv[2])

    # Check if there's a 'services' directory within the given base directory.
    # This assumes that if a services dir exists, you want to use it.
    services_path = os.path.join(path_root, "app")
    if os.path.isdir(services_path):
        base_dir = services_path

    if base_dir not in sys.path:
        sys.path.insert(0, base_dir)

    generate_service_routes(schema_file, path_root)
