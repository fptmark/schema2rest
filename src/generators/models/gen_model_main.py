#!/usr/bin/env python3
import json
import sys
from typing import Dict, Any, List
from pathlib import Path
from enum import Enum
from .validate import type_annotation, generate_enum_class
from .model_utils import dictionary_resolve
import pprint

import common.template as template
import common.helpers as helpers
from common.schema import Schema
# from utilities import utils

class Operation(Enum):
    GET = "get"
    POST = "post"
    PUT = "put"


BASE_DIR = Path(__file__).resolve().parent


def build_vars(entity: str, e_def: Dict[str, Any], templates: template.Templates, schema: Schema, operation: Operation) -> Dict[str, Any]:
    """Collect all of the placeholders your templates expect."""
    # Add the id to the metadata
    # fields =  { 'id' : { 'type': 'ObjectId', 'autoGenerate': True }  } 

    fields = e_def.get("fields", {}) 
    operations = e_def.get("operations", "")
    ui = e_def.get("ui", {})
    services = e_def.get("service", {})
    uniques = e_def.get("unique", {})

    # metadata block
    metadata = {'fields': fields, 'operations': operations, 'ui': ui, 'services': services, 'uniques': uniques}
    # replace any dictionary lookups in the metadata
    update_metadata(metadata, schema)

    # Build the Fields declarations, save function and validators
    base_field_lines = []   # declarations for fields that are not autoGenerate or autoUpdate
    auto_generate_lines: List[str] = []   # declarations for fields that are autoGenerate
    auto_update_lines: List[str] = []         # save function which is for autoUpdate fields
    enum_lines = []
    # validator_lines: List[str] = []    # validators for fields that are not autoGenerate or autoUpdate

    # Add the id to the metadata and fields
    # if operation == Operation.GET:
    #     base_field_lines.append("id: str")

    for field_name, info in fields.items():
        auto_field: bool = info.get("autoGenerate", False) or info.get("autoUpdate", False)
        if info.get('enum') is not None:
            # For enums, we need to use the enum type
            required = "" if info.get('required', False) else " | None" # required modifier
            # For UPDATE operations, all fields are optional
            if operation == Operation.PUT or not info.get('required', False):
                field_init = "Field(default=None)"
            else:
                field_init = "Field(...)"
            line = f"{field_name}: {field_name.capitalize()}Enum{required} = {field_init}"
            # Filter out autoGenerate and autoUpdate fields for CREATE/UPDATE operations
            if operation == Operation.GET or not auto_field:
                base_field_lines.append(line)
            enum_lines.extend(generate_enum_class(field_name, info['enum']['values']))
        else:
            base, init = type_annotation(info, schema, operation)
            # if field_name != "id":
            line = f"{field_name}: {base} = {init}"
            if info.get("autoGenerate", False):
                auto_generate_lines.append(line)
                # if "date" in info.get("type", "").lower():
                #     auto_generate_lines.append(f"data['{field_name}'] = datetime.now(timezone.utc)")
            elif info.get("autoUpdate", False):   # autoupdate needs to be in save()
                auto_update_lines.append(line)
                # if "date" in info.get("type", "").lower():
                #     auto_update_lines.append(f"data['{field_name}'] = datetime.now(timezone.utc)")
            else:
                # Filter out autoGenerate and autoUpdate fields for CREATE/UPDATE operations
                if operation == Operation.GET or not auto_field:
                    base_field_lines.append(line)
                    # validator_lines = validator_lines + build_validator(field_name, info, schema)

    vars = {
        "Entity":               entity,
        "EntityLower":          entity.lower(),
        "Metadata":             pprint.pformat(metadata, indent=4, sort_dicts=False),
        "BaseFields":           base_field_lines, 
        "AutoGenerateFields":   auto_generate_lines, 
        "AutoUpdateFields":     auto_update_lines, 
        "UniqueList":           json.dumps(e_def.get("unique", [])),
        "EnumClasses":          '\n'.join(enum_lines)
        # "MappingsDict":     pprint.pformat(get_elastic_search_mapping(entity, fields, schema), indent=4),
    }

    # generate the save function if there are any autoUpdate fields
    if len(auto_update_lines) > 0:
        vars["AutoUpdateLines"] =  auto_update_lines 
        # save_lines = templates.render("save", vars)
        # vars["SaveFunction"] = save_lines

    # generate the validators if there are any 
    # if len(validator_lines) > 0:
    #     vars["Validators"] = validator_lines

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



def generate_models(schema_file: str, path_root: str):

    templates = template.Templates(BASE_DIR / "..", "models")

    print(f"Generating models in {path_root}")
    schema = Schema(schema_file)
    for entity, defs in schema.concrete_entities().items():
        if defs.get("abstract", False):
            continue

        create_vars = build_vars(entity, defs, templates, schema, Operation.POST)
        create_class: List[str] =  templates.render("create", create_vars)

        update_vars = build_vars(entity, defs, templates, schema, Operation.PUT)
        update_class =  templates.render("update", update_vars)

        vars = build_vars(entity, defs, templates, schema, Operation.GET)
        vars["CreateClass"] = create_class
        vars["UpdateClass"] = update_class
        out: List[str] =  templates.render("base", vars)
        out.append("")

        helpers.write(path_root, "models", f"{entity.lower()}_model.py", out)
        # print(f"Generated {entity.lower()}_model.py")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Usage: python {sys.argv[0]} <schema.yaml> <output_path>")
        sys.exit(1)
    
    schema_file = sys.argv[1]
    path_root = sys.argv[2]

    generate_models(schema_file, path_root)
