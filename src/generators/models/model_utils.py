#!/usr/bin/env python3
from typing import Dict, Any

# sys.path.append(str(Path(__file__).parent.parent))
from common import Schema     # ← use your Schema wrapper


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

