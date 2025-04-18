#!/usr/bin/env python
import sys
import os
from pathlib import Path
from jinja2 import Environment, FileSystemLoader
import pprint
from black import format_str, FileMode

# Add parent directory to path to allow importing helpers
sys.path.append(str(Path(__file__).parent.parent))
from common import Schema
import json

DICTIONARY_KEY = "dictionary="

############################
# CUSTOM FILTER
############################
def model_field_filter(field_data):
    base_type = "Any"
    default = "..."
    field_params = []

    field_type = field_data.get('type', 'String')
    required = field_data.get('required', True)

    if not required:
        default = "None"

    # Map YAML types to Python types
    type_map = {
        'String': 'str',
        'Integer': 'int',
        'Number': 'float',
        'Boolean': 'bool',
        'ISODate': 'datetime',
        'ObjectId': 'PydanticObjectId',
        'Array[String]': 'List[str]',
        'JSON': 'dict',
    }

    base_type = type_map.get(field_type, 'Any')

    if field_type == 'ISODate':
        if field_data.get('autoGenerate') or field_data.get('autoUpdate'):
            default = "default_factory=lambda: datetime.now(timezone.utc)"

    if field_type == 'Array[String]':
        base_type = 'Optional[List[str]]' if not required else 'List[str]'

    # Field constraints
    if field_type in ['String', 'str']:
        if 'minLength' in field_data:
            field_params.append(f"min_length={field_data['minLength']}")
        if 'maxLength' in field_data:
            field_params.append(f"max_length={field_data['maxLength']}")
    if 'pattern' in field_data:
        pattern_val = field_data['pattern']
        if isinstance(pattern_val, dict):
            regex = pattern_val.get("regex")
            if regex and regex.startswith("dictionary="):
                resolved = get_dictionary_value(regex[len("dictionary="):])
                if resolved:
                    field_data['pattern']['regex'] = resolved
                    regex = resolved
            field_params.append(f"regex=r\"{regex}\"")
        else:
            field_params.append(f"regex=r'{pattern_val}'")
    if 'enum' in field_data:
        enum_val = field_data['enum']
        if isinstance(enum_val, dict):
            field_params.append(f"description=\"Allowed values: {enum_val.get('values')}\"")
        else:
            field_params.append(f"description=\"Allowed values: {enum_val}\"")
    if 'min' in field_data:
        field_params.append(f"ge={field_data['min']}")
    if 'max' in field_data:
        field_params.append(f"le={field_data['max']}")

    param_str = ", ".join(field_params)
    field_code = f"Field({default}{', ' + param_str if param_str else ''})"

    # Just return the Field(...) code
    return base_type, field_code


def extract_metadata(fields):
    """
    Extract UI metadata from fields for use in the API responses.
    Also generates any missing UI metadata with sensible defaults.
    """
    metadata = {}
    for field_name, field_value in fields.items():
        field_metadata = {}
        for key, value in field_value.items():
            if key not in ['enum', 'ui_metadata']:
                field_metadata[key] = value
            elif key == 'ui_metadata':
                field_metadata.update(value)
            elif key == "enum":
                field_metadata[key] = value

        metadata[field_name] = field_metadata

    return metadata

def combine_filter(dict1, dict2):
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
        keep_trailing_newline=True,
        trim_blocks=True,
        lstrip_blocks=True,
        extensions=['jinja2.ext.do']
    )
    env.filters['combine'] = combine_filter
    env.filters['split'] = lambda s, sep=None: s.split(sep)
    env.filters['pprint'] = lambda value, indent=4: pprint.pformat(value, indent=indent)

    return env

############################
# MODEL GENERATION
############################
def generate_models(path_root: str):
    print("Generating models...")
    models_dir = os.path.join(path_root, "app", "models")
    env = get_jinja_env()
    env.filters['model_field'] = model_field_filter

    try:
        model_template = env.get_template("model.j2")
    except Exception as e:
        print("Error loading model.j2 template:", e)
        return

    os.makedirs(models_dir, exist_ok=True)
    
    # Iterate over all entities dynamically (no special-case for any name)
    for entity_name, entity_def in schema.concrete_entities().items():
        print(f"Generating model for {entity_name}...")
        fields = entity_def.get("fields", {})
        uniques = entity_def.get("uniques", [])
        
        # Extract metadata for this entity
        metadata = {
            "entity": entity_name,
            "ui": entity_def.get("ui", entity_name),
            "operations": entity_def.get("operations", ''),
            "fields": extract_metadata(fields),
        }
        
        # for f in fields.items():
            # print(f"Field: {f}")
        auto_generate_fields = [f for f, info in fields.items() if info.get("autoGenerate")]
        auto_update_fields   = [f for f, info in fields.items() if info.get("autoUpdate")]

        rendered_model = model_template.render(
            entity=entity_name,
            fields=fields,
            uniques=uniques,
            auto_update_fields=auto_update_fields,
            auto_generate_fields=auto_generate_fields,
            services=schema.services(),  # Pass the _services mapping from the YAML
            metadata=metadata  # Pass the metadata
        )
        
        formatted_model = format_str(rendered_model, mode=FileMode())

        out_filename = f"{entity_name.lower()}_model.py"
        out_path = os.path.join(models_dir, out_filename)
        with open(out_path, "w") as f:
            f.write(formatted_model)

def get_parents(child_name, schema):
    parents = []
    for entity_name, entity_def in schema.concrete_entities().items():
        for relation in entity_def.get("relationships", []):
            if child_name == relation:
                parents.append(entity_name)
    return parents

def get_dictionary_value(value):
    words = value.split('.')
    return schema.dictionaries().get(words[0], {}).get(words[1], value)

Valid_Attribute_Messages = ["minLength.message", "maxLength.message", "pattern.message", "enum.message"]
def valid_message_fields(fields):
    bad = []
    for field, attributes in fields.items():
        for attribute in attributes:
            if '.message' in attribute and (attribute not in Valid_Attribute_Messages):
                bad.append(f"{field}:{attribute}")
    return bad

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python gen_models.py <schema.yaml> <path_root>")
        sys.exit(1)
    schema = Schema(sys.argv[1])
    path_root = sys.argv[2]
    generate_models(path_root)
