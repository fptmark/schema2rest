#!/usr/bin/env python
import sys
import os
import importlib
import importlib.util
import inspect
from pathlib import Path
from typing import Dict
from jinja2 import Environment, FileSystemLoader
from pydantic import BaseModel

# Add parent directory to path to allow importing helpers
from common import Schema
from common.helpers import write

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

    abstract_service_dir = Path(__file__).resolve().parent / "../services"  # Where the abstract services are located
    # output_dir = Path(path_root) / "app" / "services"

    schema = Schema(schema_file)
    env = get_jinja_env()
    entities = schema.concrete_entities()

    print("Generating service routes...")
    # Process each entity from the schema.
    for entity_name, entity_def in entities.items():
        services = entity_def.get("service", [])
        for service in services:
            service_parts = service.split('.')
            if len(service_parts) < 2:
                print(f"Service string '{service}' is not in expected format; expected x.y; skipping.")
                continue

            provider_name = service_parts[-1]
            concrete_class_name = provider_name.capitalize()
            alias_name = f"{concrete_class_name}_{entity_name.lower()}"

            service_dir = abstract_service_dir / service_parts[0]   # location of contracts for the service
            base_router_path = service_dir / "base_router.py"
            base_model_path = service_dir / "base_model.py"
            concrete_module_path = abstract_service_dir / Path(*service_parts[:-1]) / f"{provider_name}_provider.py" # abstract service provider

            if not base_router_path.exists() or not base_model_path.exists() or not concrete_module_path.exists():
                print(f"Skipping {entity_name} - missing one of the required files.")
                continue

            # Load all classes
            router_classes = load_classes_from_path(base_router_path)
            model_classes = load_classes_from_path(base_model_path)
            provider_classes = load_classes_from_path(concrete_module_path)

            # Build lookup
            model_class_names = {
                cls.__name__
                for cls in model_classes
                if isinstance(cls, type) and issubclass(cls, BaseModel)
            }
            if not model_class_names:
                raise Exception(f"No valid Pydantic models found in {base_model_path}")

                          # Build endpoint contract from base_router
            contract_methods = {}
            for router_cls in router_classes:
                for name, method in inspect.getmembers(router_cls, predicate=inspect.isfunction):
                    metadata = getattr(method, "_endpoint_metadata", None)
                    if metadata:
                        contract_methods[name] = {
                            "signature": inspect.signature(method),
                            "metadata": metadata
                        }

            # Build map of provider methods
            provider_methods = {
                name: inspect.signature(method)
                for cls in provider_classes
                for name, method in inspect.getmembers(cls, predicate=inspect.isfunction)
            }

            # Enforce contract conformance
            endpoints = []
            for name, contract in contract_methods.items():
                if name not in provider_methods:
                    raise Exception(f"Provider is missing required method: {name}")

                contract_sig = contract["signature"]
                provider_sig = provider_methods[name]

                if len(contract_sig.parameters) != len(provider_sig.parameters):
                    raise Exception(f"Signature mismatch on {name}: {contract_sig} != {provider_sig}")

                for param in contract_sig.parameters.values():
                    if isinstance(param.annotation, type) and issubclass(param.annotation, BaseModel):
                        if param.annotation.__name__ not in model_class_names:
                            raise Exception(f"Model {param.annotation.__name__} not found in base_model for method {name}")

                endpoints.append({
                    "name": name,
                    "metadata": contract["metadata"]
                })

            # Final rendering
            module_path = ".".join(["app", "services"] + service_parts)

            rendered = env.get_template("service_routes.j2").render(
                entity=entity_name,
                module_path=module_path,
                concrete_class=concrete_class_name,
                alias=alias_name,
                top_service=service_parts[0],
                endpoints=endpoints
            )
            
            output_dir = os.path.join(path_root, "services")
            write(path_root, output_dir, f"{alias_name.lower()}.py", rendered)
            print(f"Generated service routes for entity '{entity_name}' using service '{service}' at {output_dir}")



def load_classes_from_path(file_path: Path):
    """Dynamically load all classes from a file path."""
    module_name = file_path.stem
    spec = importlib.util.spec_from_file_location(module_name, file_path)

    if spec is None or spec.loader is None:
        print(f"ERROR: Could not load spec from {file_path}")
        return []

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return [cls for _, cls in inspect.getmembers(module, inspect.isclass)]


def get_signature_map(cls) -> Dict[str, inspect.Signature]:
    return {
        name: inspect.signature(method)
        for name, method in inspect.getmembers(cls, predicate=inspect.isfunction)
    }


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(f"Usage: python {sys.argv[0]} <schema.yaml> <output_path>")
        sys.exit(1)
    schema_file = sys.argv[1]
    path_root = os.path.abspath(sys.argv[2])
    
    generate_service_routes(schema_file, path_root)
