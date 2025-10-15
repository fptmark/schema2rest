#!/usr/bin/env python3
from typing import Dict, Any, List, Optional, Tuple
from pathlib import Path
from .model_utils import get_pattern

from common import Schema     # ← use your Schema wrapper

    
def get_constraint(info: Dict[str, Any], schema: Schema) -> List[str]:
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

def generate_enum_class(field_name: str, values: List[str]) -> List[str]:
    lines: List[str] = [f"class {field_name.capitalize()}Enum(str, Enum):"]
    lines.extend(f"    {value.upper()} = '{value}'" for value in values)
    lines.extend(" ")
    return lines


def type_annotation(info: Dict[str, Any], schema, operation):
    """Return (python_type, field_init) for a schema field."""
    t = info.get("type")
    required = info.get("required", False)
    auto_gen = info.get("autoGenerate", False)
    auto_up  = info.get("autoUpdate", False)
    auto_field: bool = auto_gen or auto_up

    # base
    if t == "Date" or t == "Datetime" or t == "DateTime":
        base = "datetime"
    elif t in ("String","str", "text"):
        base = "str"
    elif t == "Integer":
        base = "int"
    elif t == "Number":
        base = "float"
    elif t == "Currency":
        base = "float" #if get_operation else "str"
    elif t == "Boolean":
        base = "bool"
    elif t == "JSON":
        base = "Dict[str, Any]"
    elif t == "Array[String]":
        base = "List[str]"
    elif t == "ObjectId":
        base = "str" 
    else:
        base = "Any"

    # optional if not required.  note auto fields are always required.  Note that autogen are ignored for update
    if (not required and not auto_field):
        base = f"{base} | None"

    # default/factory
    validators = get_constraint(info, schema) 
    init = ""
    if auto_field and base == "datetime":
        init = "default_factory=lambda: datetime.now(timezone.utc)"
    elif base == "bool":
        init = "..., strict=True"
    elif required and not auto_field:
        init = "..., " + ', '.join(validators) if validators else "..."
    else:
        init = "default=" + ("None, " + ', '.join(validators) if validators else "None")

    return base, f"Field({init})"