#!/usr/bin/env python
import sys
import os
import importlib
import inspect
from pathlib import Path
from jinja2 import Environment, FileSystemLoader

# Add parent directory to path to allow importing helpers
sys.path.append(str(Path(__file__).parent.parent))
from common import Schema

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

    # Process each entity from the schema.
    for entity_name, entity_def in entities.items():
        inherits = entity_def.get("inherits", [])
        for base in inherits:
            if isinstance(base, dict) and "service" in base:
                for service in base["service"]:
                    if not isinstance(service, str):
                        continue
                    parts = service.split('.')
                    if len(parts) < 2:
                        print(f"Service string '{service}' is not in expected format; skipping.")
                        continue

                    # Preserve these 4 lines exactly:
                    base_module_path = "services." + parts[0] + ".base_router"
                    base_class = f"Base{parts[0].capitalize()}"
                    concrete_module_path = "services." + service  # e.g., "services.auth.cookies.redis"
                    concrete_class = parts[1].capitalize() + "Auth"  # e.g., "CookiesAuth"

                    # Compute the base_model_module for response models.
                    base_model_module = "services." + parts[0] + ".base_model"

                    # Import the abstract service class from base_router.
                    try:
                        mod = importlib.import_module(base_module_path)
                        svc_class = getattr(mod, base_class)
                    except Exception as e:
                        print(f"Error importing {base_module_path}.{base_class}: {e}")
                        continue

                    # Import the base_model module and build a mapping of endpoint -> response model.
                    response_model_mapping = {}
                    try:
                        bm_mod = importlib.import_module(base_model_module)
                        for name, cls in inspect.getmembers(bm_mod, inspect.isclass):
                            if hasattr(cls, "_expose_response"):
                                # _expose_response is a dict with key "endpoint"
                                ep = getattr(cls, "_expose_response").get("endpoint")
                                if ep:
                                    response_model_mapping[ep] = cls.__name__
                    except Exception as e:
                        print(f"Error importing {base_model_module}: {e}")
                        response_model_mapping = {}

                    # Inspect the abstract service class for methods decorated with _expose_endpoint.
                    endpoints = []
                    for name, func in inspect.getmembers(svc_class, predicate=inspect.isfunction):
                        if hasattr(func, "_expose_endpoint"):
                            metadata = getattr(func, "_expose_endpoint")
                            if not metadata.get("route"):
                                metadata["route"] = "/" + name.lower()
                            # For response model, try to match by endpoint route
                            resp_model = response_model_mapping.get(metadata["route"], None)
                            if resp_model:
                                metadata["response_model"] = resp_model
                            else:
                                # Fallback: if no matching response model found, leave empty.
                                metadata["response_model"] = ""
                            endpoints.append({
                                "name": name,
                                "metadata": metadata
                            })
                    if not endpoints:
                        continue

                    # Prepare output directory: routes/services/<top_service>
                    out_dir = os.path.join(path_root, "routes", "services", parts[0])
                    os.makedirs(out_dir, exist_ok=True)
                    out_filename = f"{entity_name.lower()}_{parts[0].lower()}_routes.py"
                    out_path = os.path.join(out_dir, out_filename)
                    
                    rendered = env.get_template("service_routes.j2").render(
                        entity=entity_name,
                        top_service=parts[0],
                        alias=parts[0].capitalize(),
                        module_path=concrete_module_path,  # use concrete module for service calls
                        concrete_class=concrete_class,
                        base_model_module=base_model_module,
                        endpoints=endpoints
                    )
                    with open(out_path, "w") as f:
                        f.write(rendered)
                    print(f"Generated routes for entity '{entity_name}' using service '{service}' at {out_path}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python gen_service_routes.py <schema.yaml> <path_root>")
        sys.exit(1)
    schema_file = sys.argv[1]
    path_root = os.path.abspath(sys.argv[2])
    
    services_path = os.path.join(path_root, "app")
    if os.path.isdir(services_path):
        path_root = services_path
    if path_root not in sys.path:
        sys.path.insert(0, path_root)
    
    generate_service_routes(schema_file, path_root)
