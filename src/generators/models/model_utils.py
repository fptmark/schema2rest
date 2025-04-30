#!/usr/bin/env python3
from typing import Dict, Any

# sys.path.append(str(Path(__file__).parent.parent))
from common import Schema     # ← use your Schema wrapper


def get_elastic_search_mapping(entity: str, fields: Dict[str, Any], schema: Schema) -> dict[str, dict]:

    m: dict[str, dict] = {}
    for name, info in fields.items():
        tp = info.get("type")
        if tp in ("String", "text"):
            # full‐text search + exact matches on .keyword subfield
            m[name] = {
                "type": "text",
                "fields": {
                    "keyword": {"type": "keyword", "ignore_above": 256}
                }
            }
        elif tp == "ISODate":
            m[name] = {
                "type": "date",
                "format": "strict_date_optional_time"
            }
        elif tp == "Integer":
            m[name] = {
                "type": "integer"
            }
        elif tp == "Number":
            m[name] = {
                "type": "double"
            }
        elif tp == "Boolean":
            m[name] = {
                "type": "boolean"
            }
        elif tp == "JSON":
            # store JSON blobs as opaque objects (no indexing of inner fields)
            m[name] = {
                "type": "object",
                "enabled": False
            }
        elif tp == "Array[String]":
            # arrays of keywords
            m[name] = {
                "type": "keyword"
            }
        elif tp == "ObjectId":
            # treat object IDs as keywords
            m[name] = {
                "type": "keyword"
            }
        else:
            # fallback to a safe keyword type
            m[name] = {
                "type": "keyword"
            }
    return m


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

