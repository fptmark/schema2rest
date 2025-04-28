#!/usr/bin/env python3
import os
import re
import json
import sys
from typing import Dict, Any, List, Optional
from pathlib import Path
from datetime import datetime, timezone
import pprint

sys.path.append(str(Path(__file__).parent.parent))
from common import Schema     # ← use your Schema wrapper

# your exact placeholder logic
PLACEHOLDER_PATTERN = re.compile(r"\{(\w+)\}")

def load_templates():
    """
    Return [(tpl_name, lines), …] sorted by leading number in filename.
    """
    template_dir = Path(__file__).resolve().parent / "templates" / "models"
    tpls = []
    for fn in sorted(template_dir.iterdir(), key=lambda p: int(p.name.split(".")[0])):
        if fn.suffix == ".tpl":
            tpls.append((fn.name, fn.read_text().splitlines()))
    return tpls


def render_template(tpl_name: str, lines: List[str], vars_map: Dict[str, str]):
    """
    Single‐pass {Key}→vars_map[Key] substitution.
    - Error if tpl_name references a var not in vars_map.
    - If a line is exactly "{Key}" and vars_map[Key]=="" skip that line.
    - If a line is exactly "{Key}" and vars_map[Key] is multiline,
      split on \n and prefix each with the same indentation,
      preserving any leading spaces in the var text.
    """
    out: List[str] = []
    for raw in lines:
        keys = PLACEHOLDER_PATTERN.findall(raw)
        missing = [k for k in keys if k not in vars_map]
        if missing:
            print(f"Template '{tpl_name}' missing vars: {missing}")
            raise RuntimeError(f"Template '{tpl_name}' missing vars: {missing}")

        stripped = raw.strip()
        # standalone placeholder?
        if len(keys) == 1 and stripped == f"{{{keys[0]}}}":
            val = vars_map[keys[0]]
            if val == "":
                continue
            indent = raw[: len(raw) - len(raw.lstrip()) ]
            for vline in val.splitlines():
                # only skip truly empty lines, keep lines that are spaces
                if vline != "":
                    out.append(indent + vline)
            continue

        # inline replacement
        line = raw
        for k in keys:
            line = line.replace(f"{{{k}}}", vars_map[k])
        out.append(line)

    return out

def type_annotation(info: Dict[str, Any], schema):
    """Return (python_type, field_init) for a schema field."""
    t = info.get("type")
    required = info.get("required", False)
    auto_gen = info.get("autoGenerate", False)
    auto_up  = info.get("autoUpdate", False)

    # base
    if t == "ISODate":
        base = "datetime"
    elif t in ("String","str", "text"):
        base = "str"
    elif t == "Integer":
        base = "int"
    elif t == "Number":
        base = "float"
    elif t == "Boolean":
        base = "bool"
    elif t == "JSON":
        base = "Dict[str, Any]"
    elif t == "Array[String]":
        base = "List[str]"
    elif t == "ObjectId":
        base = "PydanticObjectId"
    else:
        base = "Any"

    # optional wrapper
    if not required and not auto_gen and not auto_up:
        base = f"Optional[{base}]"

    # default/factory
    validators = get_validator(info, schema)
    if auto_gen or auto_up:     # assume type is ISODate
        init = "default_factory=lambda: datetime.now(timezone.utc)"
    elif required:
        init = "..., " + ', '.join(validators) if validators else "..."
    else:
        init = "None, " + ', '.join(validators) if validators else "None"

    return base, f"Field({init})"

def build_vars(entity: str, e_def: Dict[str, Any], schema: Schema):
    """Collect all of the placeholders your 1.–4. templates expect."""
    fields = e_def.get("fields", {})
    operations = e_def.get("operations", "")
    ui = e_def.get("ui", {})

    E = entity
    e = entity.lower()

    # metadata block
    metadata = {'entity': E, 'fields': fields, 'operations': operations, 'ui': ui}
    # replace any dictionary lookups in the metadata
    update_metadata(metadata, schema)
    Metadata = pprint.pformat(metadata, indent=4)

    # Build the Fields declarations, save function and validators
    base_field_lines = []   # declarations for fields that are not autoGenerate or autoUpdate
    auto_field_lines = []   # declarations for fields that are autoGenerate or autoUpdate
    save_lines = []         # save function which is for autoUpdate fields
    validator_lines: List[str] = []    # validators for fields that are not autoGenerate or autoUpdate

    for field_name, info in fields.items():
        base, init = type_annotation(info, schema)
        line = f"{field_name}: {base} = {init}"
        if info.get("autoGenerate", False):
            auto_field_lines.append(line)
        elif info.get("autoUpdate", False):   # autoupdate needs to be in save()
            auto_field_lines.append(line)
            save_lines.append(f"    self.{field_name} = datetime.now(timezone.utc)")
        else:
            base_field_lines.append(line)
            validator_lines = validator_lines + build_validator(field_name, info, schema)

    # one blank line between each field decl
    BaseFields = "\n".join(base_field_lines)
    AutoFields = "\n".join(auto_field_lines)

    # generate the save function if there are any autoUpdate fields
    if len(auto_field_lines) > 0:
        save_lines = [ "async def save(self, *args, **kwargs):" ] + save_lines 
        save_lines.append("    return await super().save(*args, **kwargs)")
        SaveFunction = "\n".join(f"{line}" for line in save_lines)

    # generate the validators if there are any 
    if len(validator_lines) > 0:
        Validators  = "\n".join(f"{line}" for line in validator_lines)


    return {
        "Entity":           E,
        "EntityLower":      e,
        "Metadata":         Metadata,
        "BaseFields":       BaseFields,
        "AutoFields":       AutoFields,
        "SaveFunction":     SaveFunction,
        "Validators":       Validators
    }

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


def get_validator(info: Dict[str, Any], schema: Schema) -> List[str]:
    lines: list[str] = []
    pydantic_type = ['min_length', 'max_length', 'ge', 'le']
    for key in pydantic_type:
        if key in info:
            lines.append(f"{key}={info[key]}")

    if 'pattern'in info:
        regex, _ = get_pattern(info, schema)
        lines.append(f"pattern=r\"{regex}\"")

    if 'enum' in info:
        enum = info['enum']
        values = enum.get('values', [])
        msg = enum.get('message', "")
        if len(msg) == 0:
            lines.append(f"description =\"{msg}: {values}\"")
        else:
            lines.append(f"description =\"{msg}\"")
    return lines

def get_pattern(info: Dict, schema: Schema) -> tuple[str, str]:
    message = ""
    pattern = info.get('pattern', {})
    if 'message' in pattern:
        message = pattern.get('message')
    if 'regex' in pattern:
        pattern = dictionary_resolve(pattern.get('regex'), schema)
    return pattern, message

def dictionary_resolve(value: str, schema: Schema) -> str:
    """
    Lookup a dictionary value in the schema.
    """
    if value.startswith("dictionary="):
        value = value.replace("dictionary=", "")
        value = schema.dictionary_lookup(value)
    return value



def build_validator(fname: str, info: Dict[str, Any], schema: Schema) -> List[str]:
    lines = []
    base, init = type_annotation(info, schema)

    # Always add ISODate pre-validator if applicable
    if info.get("type") == "ISODate":
        lines.append(f"@validator('{fname}', pre=True)")
        lines.append(f"def parse_{fname}(cls, v):")
        lines.append(f"    if v in (None, '', 'null'):")
        lines.append(f"        return None")
        lines.append(f"    if isinstance(v, str):")
        lines.append(f"        return datetime.fromisoformat(v)")
        lines.append(f"    return v")
        lines.append("")   # Blank line after pre-validator

    # Check for other constraints
    pat   = info.get("pattern")
    enum  = info.get("enum")
    mnlen = info.get("min_length")
    mxlen = info.get("max_length")
    mn    = info.get("ge")
    mx    = info.get("le")

    if any((pat, enum, mnlen is not None, mxlen is not None, mn is not None, mx is not None)):
        lines.append(f"@validator('{fname}')")
        lines.append(f"def validate_{fname}(cls, v):")
        lines.append(f"    _custom = {{}}")

        if mnlen is not None:
            lines.append(f"    if v is not None and len(v) < {mnlen}:")
            lines.append(f"        raise ValueError('{fname} must be at least {mnlen} characters')")

        if mxlen is not None:
            lines.append(f"    if v is not None and len(v) > {mxlen}:")
            lines.append(f"        raise ValueError('{fname} must be at most {mxlen} characters')")

        if pat is not None:
            if isinstance(pat, dict):
                regex, pm = get_pattern(info, schema)
            else:
                regex = pat
                pm = None
            lines.append(f"    if v is not None and not re.match(r'{regex}', v):")
            if pm:
                lines.append(f"        raise ValueError('{pm}')")
            else:
                lines.append(f"        raise ValueError('{fname} is not in the correct format')")

        if enum is not None:
            if isinstance(enum, dict):
                allowed = enum.get("values")
                em = enum.get("message")
            else:
                allowed = enum
                em = None
            lines.append(f"    allowed = {allowed}")
            lines.append(f"    if v is not None and v not in allowed:")
            if em:
                lines.append(f"        raise ValueError('{em}')")
            else:
                lines.append(f"        raise ValueError('{fname} must be one of ' + ','.join(allowed))")

        if mn is not None:
            lines.append(f"    if v is not None and v < {mn}:")
            lines.append(f"        raise ValueError('{fname} must be at least {mn}')")

        if mx is not None:
            lines.append(f"    if v is not None and v > {mx}:")
            lines.append(f"        raise ValueError('{fname} must be at most {mx}')")

        lines.append(f"    return v")
        lines.append(" ")  # Blank line after validator

    return lines


def main():
    schema = Schema(sys.argv[1])
    outdir = Path(sys.argv[2], "app", "models")
    templates = load_templates()
    outdir.mkdir(exist_ok=True)

    print(f"Generating models in {outdir}")
    for entity, defs in schema.concrete_entities().items():
        if defs.get("abstract", False):
            continue

        vars_map = build_vars(entity, defs, schema)
        out: List[str] = []

        for tpl_name, tpl_lines in templates:
            rendered = render_template(tpl_name, tpl_lines, vars_map)
            out.extend(rendered)
            out.append("")   # blank line between template blocks

        out_path = outdir / f"{entity.lower()}_model.py"
        out_path.write_text("\n".join(out).rstrip() + "\n")
        print(f"Generated Model {out_path}")

if __name__ == "__main__":
    main()
