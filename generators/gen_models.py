#!/usr/bin/env python
import sys
import os
from pathlib import Path
import json

# Add parent directory to path to allow importing helpers
sys.path.append(str(Path(__file__).parent.parent))
from common import Schema, helpers

DICTIONARY_KEY = "dictionary="

############################
# CUSTOM FILTER
############################
# def model_field_filter(field_data):
#     base_type = "Any"
#     default = "..."
#     field_params = []

#     field_type = field_data.get('type', 'String')
#     required = field_data.get('required', True)

#     if not required:
#         default = "None"

#     # Map YAML types to Python types
#     type_map = {
#         'String': 'str',
#         'Integer': 'int',
#         'Number': 'float',
#         'Boolean': 'bool',
#         'ISODate': 'datetime',
#         'ObjectId': 'PydanticObjectId',
#         'Array[String]': 'List[str]',
#         'JSON': 'dict',
#     }

#     base_type = type_map.get(field_type, 'Any')

#     if field_type == 'ISODate':
#         if field_data.get('autoGenerate') or field_data.get('autoUpdate'):
#             default = "default_factory=lambda: datetime.now(timezone.utc)"

#     if field_type == 'Array[String]':
#         base_type = 'Optional[List[str]]' if not required else 'List[str]'

#     # Field constraints
#     if field_type in ['String', 'str']:
#         if 'minLength' in field_data:
#             field_params.append(f"min_length={field_data['minLength']}")
#         if 'maxLength' in field_data:
#             field_params.append(f"max_length={field_data['maxLength']}")
#     if 'pattern' in field_data:
#         pattern_val = field_data['pattern']
#         if isinstance(pattern_val, dict):
#             regex = pattern_val.get("regex")
#             if regex and regex.startswith("dictionary="):
#                 resolved = get_dictionary_value(regex[len("dictionary="):])
#                 if resolved:
#                     field_data['pattern']['regex'] = resolved
#                     regex = resolved
#             field_params.append(f"regex=r\"{regex}\"")
#         else:
#             field_params.append(f"regex=r'{pattern_val}'")
#     if 'enum' in field_data:
#         enum_val = field_data['enum']
#         if isinstance(enum_val, dict):
#             field_params.append(f"description=\"Allowed values: {enum_val.get('values')}\"")
#         else:
#             field_params.append(f"description=\"Allowed values: {enum_val}\"")
#     if 'min' in field_data:
#         field_params.append(f"ge={field_data['min']}")
#     if 'max' in field_data:
#         field_params.append(f"le={field_data['max']}")

#     param_str = ", ".join(field_params)
#     field_code = f"Field({default}{', ' + param_str if param_str else ''})"

#     # Just return the Field(...) code
#     return base_type, field_code


# def extract_metadata(fields):
#     """
#     Extract UI metadata from fields for use in the API responses.
#     Also generates any missing UI metadata with sensible defaults.
#     """
#     metadata = {}
#     for field_name, field_value in fields.items():
#         field_metadata = {}
#         for key, value in field_value.items():
#             if key not in ['enum', 'ui_metadata']:
#                 field_metadata[key] = value
#             elif key == 'ui_metadata':
#                 field_metadata.update(value)
#             elif key == "enum":
#                 field_metadata[key] = value

#         metadata[field_name] = field_metadata

#     return metadata

# def combine_filter(dict1, dict2):
#     new_dict = dict1.copy()
#     new_dict.update(dict2)
#     return new_dict

############################
# JINJA ENVIRONMENT SETUP
############################
# def get_jinja_env() -> Environment:
#     script_dir = os.path.dirname(os.path.abspath(__file__))
#     template_dir = os.path.join(script_dir, "templates", "models")
#     env = Environment(
#         loader=FileSystemLoader(template_dir),
#         keep_trailing_newline=True,
#         trim_blocks=True,
#         lstrip_blocks=True,
#         extensions=['jinja2.ext.do']
#     )
#     env.filters['combine'] = combine_filter
#     env.filters['split'] = lambda s, sep=None: s.split(sep)
#     env.filters['pprint'] = lambda value, indent=4: pprint.pformat(value, indent=indent)

#     return env

############################
# MODEL GENERATION
############################
def generate_models(path_root: str):
    print("Generating models...")
    models_dir = os.path.join(path_root, "app", "models")

    template_dir = os.path.join('.', "templates", "models")
    templates = helpers.load_templates(template_dir)
    
    os.makedirs(models_dir, exist_ok=True)
    
    # Iterate over all entities dynamically (no special-case for any name)
    for entity_name, entity_def in schema.concrete_entities().items():
        # use a dict, not a list
        ctx = get_variables(entity_name, entity_def)
            # add whatever other slots your templates expect...

        print(f"Generating model for {entity_name}…")
        rendered_outputs = []
        for tpl_name, tpl_body in templates:
            rendered = helpers.render_template_content(tpl_body, ctx)
            rendered_outputs.append(rendered)        

            out_filename = f"{entity_name.lower()}_model.py"
            out_path = os.path.join(models_dir, out_filename)
            with open(out_path, "w") as f:
                f.write("\n\n".join(rendered_outputs))

def get_variables(entity_name, entity_def):
    regular_fields = entity_def.get("fields", {})
    autogen_fields = [f for f in regular_fields if regular_fields[f].get("autoGenerate")]
    auto_update_fields = [f for f in regular_fields if regular_fields[f].get("autoUpdate")]
    for f in regular_fields:
        if f in autogen_fields or f in auto_update_fields:
            regular_fields.pop(f)
    regular_fields_str = []
    for name, attributes in regular_fields.items():
        if attributes.required:
            string = f'    {name}: {attributes.type} = Field(None)'
        else:
            string = f'    {name}: Optional[{attributes.type}] = Field(None)'
        if attributes.type == "ISODate":
            string
    
    return {
            "Entity": entity_name,
            "entity": entity_name.lower(),
            "fields":   entity_def.get("fields", {}),
            "metadata": entity_def.get("metadata", {}),
            "uniques":  entity_def.get("unique", []),
            "services": schema.services(),
    }

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




#  New generator

import yaml
import json
from string import Template
from datetime import datetime, timezone
from pathlib import Path

def load_schema(path):
    return yaml.safe_load(Path(path).read_text())

def build_field_defs(fields):
    lines = []
    for name, info in fields.items():
        # determine python type & default
        # Here you should integrate your model_field helper
        py_type, default = info.get("python_type"), info.get("default")
        if not info.get("required"):
            line = f"    {name}: Optional[{py_type}] = {default}"
        else:
            line = f"    {name}: {py_type} = {default}"
        lines.append(line)
    return "\n".join(lines)

def build_validators(fields):
    blocks = []
    for name, info in fields.items():
        if info.get("type") == "ISODate":
            blocks.append(f"""    @validator('{name}', pre=True)
    def parse_{name}(cls, v):
        if v in (None, '', 'null'):
            return None
        if isinstance(v, str):
            return datetime.fromisoformat(v)
        return v
""")
    return "\n".join(blocks)

def build_save_method(auto_upd, auto_gen, uniques):
    parts = []
    if uniques:
        parts.append("async def validate_uniques(self):")
        for idx,u in enumerate(uniques, start=1):
            q = ", ".join(f'\"{f}\": self.{f}' for f in u)
            parts.append(f"    query_{idx} = {{ {q} }}")
            parts.append(f"    existing_{idx} = await self.__class__.find_one(query_{idx})")
            parts.append(f"    if existing_{idx}:")
            parts.append(f"        raise UniqueValidationError({u!r}, query_{idx})")
        parts.append("")

    if auto_gen:
        parts.append("@before_event(Insert)")
        parts.append("def _set_autogen(self):")
        parts.append("    now = datetime.now(timezone.utc)")
        for f in auto_gen:
            parts.append(f"    self.{f} = now")
        parts.append("")

    parts.append("async def save(self, *args, **kwargs):")
    if uniques:
        parts.append("    await self.validate_uniques()")
    if auto_upd:
        parts.append("    current_time = datetime.now(timezone.utc)")
        for f in auto_upd:
            parts.append(f"    self.{f} = current_time")
    parts.append("    return await super().save(*args, **kwargs)")
    return "\n".join(parts)

def render_template(tpl_path, **ctx):
    raw = Path(tpl_path).read_text()
    return Template(raw).substitute(**ctx)

def main():
    schema = load_schema('schema.yaml')['_entities']
    for entity, meta in schema.items():
        fields = {}
        if meta.get('inherit'):
            base = schema['BaseEntity']['fields']
            fields.update(base)
        fields.update(meta['fields'])
        auto_upd = [n for n,i in fields.items() if i.get('autoUpdate')]
        auto_gen = [n for n,i in fields.items() if i.get('autoGenerate')]
        uniques = meta.get('unique', [])
        fd = build_field_defs(fields)
        md_json = json.dumps(meta, indent=4)
        save_block = build_save_method(auto_upd, auto_gen, uniques)
        vals = build_validators(fields)

        base_ctx = {
            'Entity': entity,
            'entity_lower': entity.lower(),
            'FieldDefs': fd,
            'Metadata': md_json,
            'SaveMethod': save_block,
            'CreateFields': "\n".join(l for l in fd.splitlines() if 'createdAt' not in l and 'updatedAt' not in l),
            'UpdateFields': "\n".join(l for l in fd.splitlines() if 'updatedAt' not in l),
            'ReadFields': fd,
            'Validators': vals
        }

        out_model  = render_template(os.path.join(templates_dir, 'model.tpl'), **base_ctx)
        out_create = render_template(os.path.join(templates_dir, 'create.tpl'), **base_ctx)
        out_update = render_template(os.path.join(templates_dir, 'update.tpl'), **base_ctx)
        out_read   = render_template(os.path.join(templates_dir, 'read.tpl'), **base_ctx)

        gen_dir = os.path.join(base_dir, 'gen')
        os.makedirs(gen_dir, exist_ok=True)
        Path(os.path.join(gen_dir, f'{entity.lower()}_model.py')).write_text(
            "\n\n".join([out_model, out_create, out_update, out_read])
        )

if __name__ == '__main__':
    main()