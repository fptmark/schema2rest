#!/usr/bin/env python
import sys
import os
from pathlib import Path
from jinja2 import Environment, FileSystemLoader

# Add parent directory to path to allow importing helpers
sys.path.append(str(Path(__file__).parent.parent))
from common import Schema
import json

DICTIONARY_KEY = "dictionary="

############################
# CUSTOM FILTER
############################
def model_field_filter(field_data: dict) -> str:
    """
    Convert a dict of validations into a Python type annotation for Pydantic.
    It now checks for autoGenerate/autoUpdate flags to produce a default_factory.
    """
    type_map = {
        'String': 'str',
        'Boolean': 'bool',
        'Number': 'float',
        'Integer': 'int',
        'ISODate': 'datetime',
        'ObjectId': 'PydanticObjectId',  # Use Beanie's type
        'JSON': 'dict',
        'Array[String]': 'List[str]',
        'Array[Integer]': 'List[int]',
        'Array[Number]': 'List[float]',
        'Array[Boolean]': 'List[bool]',
        'Array[ISODate]': 'List[datetime]',
        'Array[ObjectId]': 'List[PydanticObjectId]',
        'Array[JSON]': 'List[dict]'
    }
    field_type = field_data.get('type', 'String')
    py_type = type_map.get(field_type, 'str')

    # Check for autoGenerate or autoUpdate flags.
    if field_data.get('autoGenerate', False) or field_data.get('autoUpdate', False):
        # For ISODate, we'll use datetime.now(timezone.utc) as the default factory.
        # You could add additional logic here for other types.
        default_str = "Field(default_factory=lambda: datetime.now(timezone.utc))"
        base_type = py_type  # Even if required is true, we supply a default.
    else:
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

def infer_widget_type(field_info):
    """
    Infer the appropriate widget type based on field type and validations.
    """
    field_type = field_info.get("type", "String")
    
    if field_type == "String":
        if "enum" in field_info:
            return "select"
        if "pattern" in field_info:
            pattern = field_info["pattern"]
            if "email" in str(pattern).lower():
                return "email"
            if "url" in str(pattern).lower():
                return "url"
        if "maxLength" in field_info and int(field_info["maxLength"]) > 100:
            return "textarea"
        return "text"
    elif field_type == "Boolean":
        return "checkbox"
    elif field_type in ["Number", "Integer"]:
        return "number"
    elif field_type == "ISODate":
        return "date"
    elif field_type == "JSON":
        return "jsoneditor"
    elif field_type.startswith("Array"):
        return "multiselect"
    elif field_type == "ObjectId":
        return "reference"
    return "text"  # Default fallback

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
                field_metadata["options"] = value

        # Set displayName (convert camelCase to Title Case if not specified) unless it was specified in the mmd/yaml
        if "displayName" not in field_metadata:
            display_name = ''.join(' ' + char if char.isupper() else char for char in field_name).strip()
            if display_name.title() != field_name:
                field_metadata["displayName"] = display_name.title()

        metadata[field_name] = field_metadata

    return metadata

### ORIGINALLY in extract_metadata

        # # Set default display mode
        # if "display" not in field_info:
        #     # Hide password fields by default in read views
        #     if field_name.lower().find("password") >= 0:
        #         field_meta["display"] = "form"
        #     else:
        #         field_meta["display"] = "always"
        # else:
        #     field_meta["display"] = field_info.get("display")
        
        # # Set displayAfterField for proper field ordering
        # if "displayAfterField" not in field_info:
        #     prev_field = field_order[i-1] if i > 0 else None
        #     field_meta["displayAfterField"] = prev_field if prev_field else ""
        # else:
        #     field_meta["displayAfterField"] = field_info.get("displayAfterField", "")
        
        # # Infer widget type if not explicitly set
        # if "widget" not in field_info:
        #     field_meta["widget"] = infer_widget_type(field_info)
        # else:
        #     field_meta["widget"] = field_info.get("widget")
            

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
        autoescape=False,
        trim_blocks=True,
        lstrip_blocks=True,
        extensions=['jinja2.ext.do']
    )
    env.filters['combine'] = combine_filter
    env.filters['split'] = lambda s, sep=None: s.split(sep)
    return env

############################
# MODEL GENERATION
############################
def generate_models(schema_file: str, path_root: str):
    print("Generating models...")
    models_dir = os.path.join(path_root, "app", "models")
    schema = Schema(schema_file)
    env = get_jinja_env()
    env.filters['model_field'] = model_field_filter

    try:
        model_template = env.get_template("model.j2")
    except Exception as e:
        print("Error loading model.j2 template:", e)
        return

    os.makedirs(models_dir, exist_ok=True)
    
    # Create a metadata directory to store the UI metadata
    # metadata_dir = os.path.join(path_root, "app", "metadata")
    # os.makedirs(metadata_dir, exist_ok=True)

    # Iterate over all entities dynamically (no special-case for any name)
    for entity_name, entity_def in schema.concrete_entities().items():
        print(f"Generating model for {entity_name}...")
        raw_inherits = entity_def.get("inherits", [])
        processed_bases = []
        for base in raw_inherits:
            if isinstance(base, dict) and "service" in base:
                for s in base["service"]:
                    if isinstance(s, str):  # Service provided as a mapping
                        alias = s.split('.')[0].capitalize()
                        processed_bases.append(alias)
                    else:
                        processed_bases.append(s)
            elif isinstance(base, str):
                processed_bases.append(base)
        if not processed_bases:
            processed_bases = ["Document"]

        fields = entity_def.get("fields", {})

        # Examine relationships for this entity to find foreign key and auto add it's id to fields
        for parent in get_parents(entity_name, schema):
            parent_id_field = f"{parent.lower()}Id"
            fields[parent_id_field] = { 
                "type": "ObjectId", 
                "required": True,
                "displayName": f"{parent} ID",
                "readOnly": True,
                # "widget": "reference",
                # "display": "always",
                # "referenceEntity": parent
            }

        # Process dictionary lookups.
        for field, attributes in fields.items():
            for attribute, value in attributes.items():
                if isinstance(value, str) and value.startswith(DICTIONARY_KEY):
                    value = value[len(DICTIONARY_KEY):]
                    # Update the attribute with the looked-up value
                    fields[field][attribute] = get_dictionary_value(schema.dictionaries(), value)

        uniques = entity_def.get("uniques", [])
        
        # Find all auto-update fields
        auto_update_fields = []
        for field_name, field_info in fields.items():
            if field_info.get('type') == 'ISODate' and field_info.get('autoUpdate', False):
                auto_update_fields.append(field_name)

        # Extract metadata for this entity
        metadata = {
            "entity": entity_name,
            "labels": entity_def.get("labels", entity_name),
            "operations": entity_def.get("operations", ''),
            "fields": extract_metadata(fields),
        }
        
        # Write metadata to JSON file
        # metadata_file = os.path.join(metadata_dir, f"{entity_name.lower()}_metadata.json")
        # with open(metadata_file, "w") as mf:
        #     json.dump(metadata, mf, indent=2)
        
        # Add metadata access to rendered model
        rendered_model = model_template.render(
            entity=entity_name,
            fields=fields,
            inherits=processed_bases,  # For class declaration
            raw_inherits=raw_inherits,  # For generating imports
            uniques=uniques,
            auto_update_fields=auto_update_fields,
            services=schema.services(),  # Pass the _services mapping from the YAML
            metadata=metadata  # Pass the metadata
        )
        
        out_filename = f"{entity_name.lower()}_model.py"
        out_path = os.path.join(models_dir, out_filename)
        with open(out_path, "w") as f:
            f.write(rendered_model)

def get_parents(child_name, schema):
    parents = []
    for entity_name, entity_def in schema.concrete_entities().items():
        for relation in entity_def.get("relationships", []):
            if child_name == relation:
                parents.append(entity_name)
    return parents

def get_dictionary_value(dictionaries, value):
    words = value.split('.')
    if dictionaries.get(words[0]):
        return dictionaries[words[0]].get(words[1], value)
    return value

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
    schema_file = sys.argv[1]
    path_root = sys.argv[2]
    generate_models(schema_file, path_root)
