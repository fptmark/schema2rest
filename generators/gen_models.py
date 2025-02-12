#!/usr/bin/env python
import sys
import os
from jinja2 import Environment, FileSystemLoader
import helpers

############################
# CUSTOM FILTER
############################
def model_field_filter(field_data: dict) -> str:
    """
    Convert a dict of validations (e.g., {'type': 'String', 'required': True, ...})
    into a Python type annotation for Pydantic.

    If a custom error message is provided (e.g. with keys like "minLength.message" or "pattern.message"),
    the corresponding constraint will be omitted from the Field() call so that a custom validator can be generated.
    """
    type_map = {
        'String': 'str',
        'Boolean': 'bool',
        'Number': 'float',
        'Integer': 'int',
        'ISODate': 'datetime',
        'ObjectId': 'PydanticObjectId',  # Use Beanie's type
        'JSON': 'dict'
    }

    field_type = field_data.get('type', 'String')
    py_type = type_map.get(field_type, 'str')

    # Convert required string to boolean.
    required_val = field_data.get('required', False)
    if isinstance(required_val, str):
        required_val = (required_val.lower() == 'true')

    if required_val:
        base_type = py_type
        default_str = "Field(..."
    else:
        base_type = f"Optional[{py_type}]"
        default_str = "Field(None"

    field_params = []
    # Only add built-in constraints if no custom message is provided.
    if 'minLength' in field_data and 'minLength.message' not in field_data:
        field_params.append(f"min_length={field_data['minLength']}")
    if 'maxLength' in field_data and 'maxLength.message' not in field_data:
        field_params.append(f"max_length={field_data['maxLength']}")
    if 'pattern' in field_data and 'pattern.message' not in field_data:
        field_params.append(f"regex=r'{field_data['pattern']}'")
    if 'enum' in field_data:
        field_params.append(f"description=\"Allowed values: {field_data['enum']}\"")
    if 'min' in field_data:
        field_params.append(f"ge={field_data['min']}")
    if 'max' in field_data:
        field_params.append(f"le={field_data['max']}")

    if field_params:
        default_str += ", " + ", ".join(field_params)
    default_str += ")"

    return f"{base_type} = {default_str}"

def combine_filter(dict1, dict2):
    # Return a new dictionary combining dict1 and dict2, with dict2's keys taking precedence.
    new_dict = dict1.copy()
    new_dict.update(dict2)
    return new_dict

############################
# JINJA ENVIRONMENT SETUP
############################
def get_jinja_env() -> Environment:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    template_dir = os.path.join(script_dir, "templates", "models")
    env = Environment(
        loader=FileSystemLoader(template_dir),
        autoescape=False,
        trim_blocks=True,
        lstrip_blocks=True,
        extensions=['jinja2.ext.do']  # Enable the do extension
    )
    env.filters['combine'] = combine_filter  # Register the combine filter
    return env

############################
# MODEL GENERATION
############################
def generate_models(schema_file: str, path_root: str):
    print("Generating models...")
    models_dir = os.path.join(path_root, "app", "models")
    schema = helpers.get_schema(schema_file)
    env = get_jinja_env()
    env.filters['model_field'] = model_field_filter

    try:
        base_entity_template = env.get_template("base_entity_model.j2")
    except Exception as e:
        base_entity_template = None

    model_template = env.get_template("model.j2")
    os.makedirs(models_dir, exist_ok=True)

    if "BaseEntity" in schema and base_entity_template:
        print("Generating BaseEntity.py...")
        base_entity_def = schema["BaseEntity"]
        base_entity_fields = base_entity_def.get("fields", {})
        uniques = base_entity_def.get("uniques", [])
        rendered_base = base_entity_template.render(entity="BaseEntity", fields=base_entity_fields, uniques=uniques)
        base_entity_path = os.path.join(models_dir, "BaseEntity.py")
        with open(base_entity_path, "w") as f:
            f.write(rendered_base)
        print("BaseEntity.py generated.")

    special_keys = ["_relationships", "BaseEntity"]
    for entity_name, entity_def in schema.items():
        if entity_name in special_keys:
            continue

        inherits = entity_def.get("inherits")
        inherits_base = True if inherits and ("BaseEntity" in inherits) else False
        fields = entity_def.get("fields", {})
        uniques = entity_def.get("uniques", [])
        rendered_model = model_template.render(
            entity=entity_name,
            fields=fields,
            inheritsBaseEntity=inherits_base,
            uniques=uniques
        )
        out_filename = f"{entity_name.lower()}_model.py"
        out_path = os.path.join(models_dir, out_filename)
        with open(out_path, "w") as f:
            f.write(rendered_model)
        print(f"Generated {out_filename}")

    print("Model generation complete!")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python gen_models.py <schema.yaml> <path_root>")
        sys.exit(1)
    schema_file = sys.argv[1]
    path_root = sys.argv[2]
    generate_models(schema_file, path_root)
