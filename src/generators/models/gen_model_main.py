#!/usr/bin/env python3
import json
import sys
from typing import Dict, Any, List
from pathlib import Path
from .validate import type_annotation, build_validator
from .model_utils import get_pattern, get_elastic_search_mapping, dictionary_resolve
import pprint

import common.template as template
import common.helpers as helpers
from common.schema import Schema
# from utilities import utils

BASE_DIR = Path(__file__).resolve().parent


def build_vars(entity: str, e_def: Dict[str, Any], templates: template.Templates, schema: Schema):
    """Collect all of the placeholders your templates expect."""
    fields = e_def.get("fields", {})
    operations = e_def.get("operations", "")
    ui = e_def.get("ui", {})

    # metadata block
    metadata = {'entity': entity, 'fields': fields, 'operations': operations, 'ui': ui}
    # replace any dictionary lookups in the metadata
    update_metadata(metadata, schema)

    # Build the Fields declarations, save function and validators
    base_field_lines = []   # declarations for fields that are not autoGenerate or autoUpdate
    auto_field_lines = []   # declarations for fields that are autoGenerate or autoUpdate
    auto_update_lines: List[str] = []         # save function which is for autoUpdate fields
    validator_lines: List[str] = []    # validators for fields that are not autoGenerate or autoUpdate

    for field_name, info in fields.items():
        base, init = type_annotation(info, schema)
        line = f"{field_name}: {base} = {init}"
        if info.get("autoGenerate", False):
            auto_field_lines.append(line)
        elif info.get("autoUpdate", False):   # autoupdate needs to be in save()
            auto_field_lines.append(line)
            auto_update_lines.append(f"self.{field_name} = datetime.now(timezone.utc)")
        else:
            base_field_lines.append(line)
            validator_lines = validator_lines + build_validator(field_name, info, schema)

    vars = {
        "Entity":           entity,
        "EntityLower":      entity.lower(),
        "Metadata":         pprint.pformat(metadata, indent=4),
        "BaseFields":       base_field_lines, 
        "AutoFields":       auto_field_lines, 
        "UniqueList":       json.dumps(e_def.get("unique", [])),
        "MappingsDict":     pprint.pformat(get_elastic_search_mapping(entity, fields, schema), indent=4),
    }

    # generate the save function if there are any autoUpdate fields
    if len(auto_update_lines) > 0:
        vars["AutoUpdateLines"] =  auto_update_lines 
        save_lines = templates.render("save", vars)
        vars["SaveFunction"] = save_lines

    # generate the validators if there are any 
    if len(validator_lines) > 0:
        vars["Validators"] = validator_lines

    return vars

def update_metadata(metadata: Dict[str, Any], schema: Schema):
    """
    Update the metadata dictionary with any dictionary lookups.
    """
    for field_name, field_def in metadata.items():
        if isinstance(field_def, dict):
            update_metadata(field_def, schema)
        elif field_name == 'regex' and isinstance(field_def, str):
            metadata['regex'] = dictionary_resolve(field_def, schema)
    return metadata



def generate_models(schema_file: str, path_root: str, backend: str):

    templates = template.Templates(BASE_DIR / "..", "models", backend)

    print(f"Generating models in {path_root}")
    schema = Schema(schema_file)
    for entity, defs in schema.concrete_entities().items():
        if defs.get("abstract", False):
            continue

        vars_map = build_vars(entity, defs, templates, schema)
        out: List[str] = []

        for i in range(1, len(templates.list())):
            rendered = templates.render(str(i), vars_map)
            out.extend(rendered)
            out.append("")   # blank line between template blocks

        helpers.write(path_root, backend, "models", f"{entity.lower()}_model.py", out)
        # print(f"Generated {entity.lower()}_model.py")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(f"Usage: python {sys.argv[0]} <schema.yaml> <output_path> [<backend>]")
        sys.exit(1)
    
    schema_file = sys.argv[1]
    path_root = sys.argv[2]
    backend = sys.argv[3] if len(sys.argv) >= 3 else "mongo"

    if helpers.valid_backend(backend):
        generate_models(schema_file, path_root, backend)
