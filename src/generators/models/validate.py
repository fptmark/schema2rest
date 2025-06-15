#!/usr/bin/env python3
from typing import Dict, Any, List, Optional, Tuple
from pathlib import Path
from .model_utils import get_pattern

from common import Schema     # ← use your Schema wrapper

    
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


def build_validator(fname: str, info: Dict[str, Any], schema: Schema) -> List[str]:
    lines = []
    convert_v = None
    # base, init = type_annotation(info, schema)

    # Always add ISODate pre-validator if applicable
    if info.get("type") == "ISODate":
        lines.append(f"@field_validator('{fname}', mode='before')")
        lines.append(f"def parse_{fname}(cls, v):")
        lines.append(f"    if cls._validate:")
        lines.append(f"        if v in (None, '', 'null'):")
        lines.append(f"            return None")
        lines.append(f"        if isinstance(v, str):")
        lines.append(f"            return datetime.fromisoformat(v)")
        lines.append(f"    return v")
        lines.append("")   # Blank line after pre-validator

    # Check for other constraints
    pat   = info.get("pattern")
    enum  = info.get("enum")
    mnlen = info.get("min_length")
    mxlen = info.get("max_length")
    mn    = info.get("ge")
    mx    = info.get("le")

    if any(v is not None for v in (pat, enum, mnlen, mxlen, mn, mx)):
        if info['type'] == "Integer":   # ToDo: only applies to mn/mx.
            convert_v = "int(v)"
        elif info['type'] in ['Number', 'Float']:
            convert_v = "float(v)"

        lines.append(f"@field_validator('{fname}', mode='before')")
        lines.append(f"def validate_{fname}(cls, v):")
        lines.append(f"    if cls._validate:")
        # lines.append(f"    _custom = {{}}")

        if info['type'] == "Currency":
            lines.append    (f"        if v is None: return None")
            lines.append    (f"        parsed = helpers.parse_currency(v)")
            lines.append    (f"        if parsed is None:")
            lines.append    (f"            raise ValueError('{fname} must be a valid currency')")
            if mn is not None:
                lines.append(f"        if parsed < {mn}:")
                lines.append(f"            raise ValueError('{fname} must be at least {mn}')")
            if mx is not None:
                lines.append(f"        if parsed > {mx}:")
                lines.append(f"            raise ValueError('{fname} must be at most {mx}')")
            lines.append    (f"    return parsed")
            return lines
            
        if mnlen:
            lines.append(f"        if v is not None and len(v) < {mnlen}:")
            lines.append(f"            raise ValueError('{fname} must be at least {mnlen} characters')")

        if mxlen:
            lines.append(f"        if v is not None and len(v) > {mxlen}:")
            lines.append(f"            raise ValueError('{fname} must be at most {mxlen} characters')")

        if pat:
            if isinstance(pat, dict):
                regex, pm = get_pattern(info, schema)
            else:
                regex = pat
                pm = None
            lines.append     (f"        if v is not None and not re.match(r'{regex}', v):")
            if pm:
                lines.append(f"            raise ValueError('{pm}')")
            else:
                lines.append(f"            raise ValueError('{fname} is not in the correct format')")

        if enum:
            if isinstance(enum, dict):
                allowed = enum.get("values")
                em = enum.get("message")
            else:
                allowed = enum
                em = None
            lines.append    (f"        allowed = {allowed}")
            lines.append    (f"        if v is not None and v not in allowed:")
            if em:
                lines.append(f"            raise ValueError('{em}')")
            else:
                lines.append(f"            raise ValueError('{fname} must be one of ' + ','.join(allowed))")

        if mn is not None:
            lines.append    (f"        if v is not None and {convert_v} < {mn}:")
            lines.append    (f"            raise ValueError('{fname} must be at least {mn}')")

        if mx is not None:
            lines.append    (f"        if v is not None and {convert_v} > {mx}:")
            lines.append    (f"            raise ValueError('{fname} must be at most {mx}')")

        lines.append        (f"    return v")
        lines.append(" ")  # Blank line after validator

    return lines

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
    elif t == "Number" or t == "Currency":
        base = "float"
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