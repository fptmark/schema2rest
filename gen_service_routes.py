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
    env.filters['split'] = lambda s, sep=None: s.split(sep)
    return env

def generate_service_routes(schema_file: str, path_root: str):
    print("Generating service routes...")
    schema = Schema(schema_file)
    env = get_jinja_env()
    entities = schema.concrete_entities()

    # For each entity, scan its inherits for a service mapping.
    for entity_name, entity_def in entities.items():
        inherits = entity_def.get("inherits", [])
        for base in inherits:
            if isinstance(base, dict) and "service" in base:
                for service in base["service"]:
                    if not isinstance(service, str):
                        continue
                    # Compute service details using the abstract base:
                    # For service string "auth.cookies.redis", we want:
                    #   module_path: "services.auth.cookies.redis"
                    #   concrete_class: (second token capitalized + "Auth") e.g., "CookiesAuth"
                    #   alias: (first token capitalized) e.g., "Auth"
                    parts = service.split('.')
                    if len(parts) < 2:
                        print(f"Service string '{service}' is not in expected format; skipping.")
                        continue
                    top_service = parts[0]    # e.g., "auth"
                    alias = top_service.capitalize()  # e.g., "Auth"
                    base_module_path = "services." + top_service + ".base_router"
                    base_class = f"Base{top_service.capitalize()}"
                    concrete_module_path = "services." + service  # e.g., "services.auth.cookies.redis"
                    concrete_class = parts[1].capitalize() + "Auth"  # e.g., "CookiesAuth"
                    
                    # Dynamically import the abstract service class.
                    try:
                        mod = importlib.import_module(base_module_path)
                        svc_class = getattr(mod, base_class)
                    except Exception as e:
                        print(f"Error importing {base_module_path}.{base_class}: {e}")
                        continue

                    # Inspect the service class for methods decorated with _expose_endpoint.
                    endpoints = []
                    for name, func in inspect.getmembers(svc_class, predicate=inspect.isfunction):
                        if hasattr(func, "_expose_endpoint"):
                            metadata = getattr(func, "_expose_endpoint")
                            # If no route is provided, leave it empty for the template to assign defaults.
                            if "route" not in metadata:
                                metadata["route"] = ""
                            endpoints.append({
                                "name": name, #f"{entity_name}{alias}{name.capitalize()}",
                                "metadata": metadata
                            })
                    if not endpoints:
                        continue

                    # Prepare output directory: routes/services/<top_service>
                    out_dir = os.path.join(path_root, "routes", "services", top_service)
                    os.makedirs(out_dir, exist_ok=True)
                    out_filename = f"{entity_name.lower()}_{top_service.lower()}_routes.py"
                    out_path = os.path.join(out_dir, out_filename)
                    
                    rendered = env.get_template("service_routes.j2").render(
                        entity=entity_name,
                        top_service=top_service,
                        alias=alias,
                        module_path=concrete_module_path,
                        concrete_class=concrete_class,
                        endpoints=endpoints
                    )
                    with open(out_path, "w") as f:
                        f.write(rendered)
                    print(f"Generated routes for entity '{entity_name}' with service '{service}' at {out_path}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python gen_service_routes.py <schema.yaml> <path_root>")
        sys.exit(1)
    schema_file = sys.argv[1]
    base_dir = os.path.abspath(sys.argv[2])

    # Environment setup block from main -- DO NOT CHANGE IT!!!!
    services_path = os.path.join(base_dir, "app")
    if os.path.isdir(services_path):
        base_dir = services_path

    if base_dir not in sys.path:
        sys.path.insert(0, base_dir)

    generate_service_routes(schema_file, base_dir)
