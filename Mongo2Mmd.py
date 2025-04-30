#!/usr/bin/env python3
"""mongo_extractor.py – v3.2

*   **BaseEntity** now marks `%% @abstract` **inside** the block, after the
    field list.
*   **Decorators order & layout** – for every entity (including BaseEntity)
    decorators (`%% @include …`, `%% @unique …`) are placed *after* the last
    field with a blank line in‑between, per your readability rule.

Other logic (pattern, enum, unique‑index inference) is unchanged.
"""
from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, List, Set

from pymongo import MongoClient

# ───────────────────────────────────────────────────────────── constants ───
EMAIL_RX = re.compile(r"^[a-zA-Z0-9._%-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
URL_RX = re.compile(r"^https?://[^\s]+$")
CANDIDATE_PATTERNS = {
    "email": (EMAIL_RX, "Bad email address format"),
    "url":   (URL_RX,   "Bad URL format"),
}

ENUM_MIN_FREQ   = 10       # each distinct value must appear ≥ 10×
ENUM_MAX_SIZE   = 15       # max enum cardinality
ENUM_COVERAGE   = 0.98     # enum must cover ≥ 98 % of non‑null rows
COMMON_THRESHOLD = 0.80    # field present in ≥ 80 % of collections
SAMPLE_SIZE      = 500     # docs to sample per collection

# ───────────────────────────────────────────────────── helper functions ───

def infer_pattern(values: List[str]):
    for _key, (rx, msg) in CANDIDATE_PATTERNS.items():
        if all(rx.fullmatch(v) for v in values if isinstance(v, str)):
            return rx.pattern, msg
    return None


def infer_enum(values: List[Any]):
    cleaned = [v for v in values if v is not None]
    if not cleaned:
        return None
    freq = Counter(cleaned)
    if any(c < ENUM_MIN_FREQ for c in freq.values()):
        return None
    if len(freq) > ENUM_MAX_SIZE:
        return None
    if sum(freq.values()) / len(values) < ENUM_COVERAGE:
        return None
    return sorted(freq)


def bson_to_dtype(types: Set[str]):
    if "datetime" in types:
        return "ISODate"
    if "ObjectId" in types:
        return "ObjectId"
    if "bool" in types:
        return "Boolean"
    if "int" in types:
        return "Integer"
    if "float" in types:
        return "Number"
    if "list" in types:
        return "Array[String]"
    if "dict" in types:
        return "JSON"
    return "String"


# ───────────────────────────────────────────────────── database connect ───
config = json.loads(Path("config.json").read_text())
client = MongoClient(config["mongo_uri"])
db = client[config["db_name"]]

collections = [
    c for c in db.list_collection_names(filter={"type": "collection"})
    if not c.startswith("system.") and c not in {"fs.files", "fs.chunks"}
]

# ───────────────────────────────────────── gather metadata per entity ───
entity_fields: Dict[str, Dict[str, Any]] = {}
entity_indexes: Dict[str, List[List[str]]] = {}
field_to_collections: Dict[str, Set[str]] = defaultdict(set)
field_global_types: Dict[str, Set[str]] = defaultdict(set)

for coll_name in collections:
    coll = db[coll_name]
    total = coll.estimated_document_count()
    if total == 0:
        continue
    docs = (
        list(coll.aggregate([{"$sample": {"size": SAMPLE_SIZE}}]))
        if total > SAMPLE_SIZE else list(coll.find())
    )

    per_field: Dict[str, Dict[str, Any]] = defaultdict(
        lambda: {"types": set(), "values": []}
    )

    for doc in docs:
        for k, v in doc.items():
            if k == "_id":
                continue
            meta = per_field[k]
            meta["types"].add(type(v).__name__)
            if isinstance(v, (str, int, float, bool)) and len(meta["values"]) < SAMPLE_SIZE:
                meta["values"].append(v)

    entity_fields[coll_name] = per_field

    for f, m in per_field.items():
        field_to_collections[f].add(coll_name)
        field_global_types[f].update(m["types"])

    entity_indexes[coll_name] = [
        [field for field, _ in idx["key"]]
        for idx in coll.index_information().values()
        if idx.get("unique")
    ]

# ───────────────────────────────────────── derive BaseEntity candidates ───
num_col = len(collections)
base_fields: List[tuple[str, str]] = []
for f, collset in field_to_collections.items():
    if len(collset) / num_col >= COMMON_THRESHOLD and len(field_global_types[f]) == 1:
        base_fields.append((f, next(iter(field_global_types[f]))))
base_field_names = {f for f, _ in base_fields}

# ─────────────────────────────────────────────── build Mermaid diagram ───
lines: List[str] = ["erDiagram"]

# ---- BaseEntity (abstract) ----
if base_fields:
    lines.append("    BaseEntity {")
    for fname, t in base_fields:
        lines.append(f"        {bson_to_dtype({t})} {fname}")
    lines.append("")
    lines.append("        %% @abstract")
    lines.append("    }\n")

# ---- Concrete entities ----
for ent, fields in entity_fields.items():
    lines.append(f"    {ent} {{")

    # ---- field lines ----
    for field, meta in fields.items():
        if field in base_field_names:
            continue
        dtype = bson_to_dtype(meta["types"])
        pattern_info = infer_pattern(meta["values"]) if dtype == "String" else None
        validate_parts = []
        if pattern_info:
            pat_src, pat_msg = pattern_info
            pat_src = pat_src.replace("\\", "\\\\")
            validate_parts.append(
                f'pattern: {{ regex: "{pat_src}", message: "{pat_msg}" }}'
            )
        validate_str = f" %% @validate {{ {', '.join(validate_parts)} }}" if validate_parts else ""
        enum_vals = infer_enum(meta["values"]) if dtype == "String" else None
        enum_str = f" %% @enum {{ {', '.join(map(str, enum_vals))} }}" if enum_vals else ""
        lines.append(f"        {dtype} {field}{validate_str}{enum_str}")

    # ---- decorators (include + unique) ----
    decorators: List[str] = []
    if base_fields:
        decorators.append("        %% @include BaseEntity")
    for idx in entity_indexes.get(ent, []):
        decorators.append(f"        %% @unique {' + '.join(idx)}")

    if decorators:
        lines.append("")  # blank line before decorators for readability
        lines.extend(decorators)

    lines.append("    }\n")

# ---- relationships based on ObjectId suffix ...Id ----
for ent, fields in entity_fields.items():
    for field, meta in fields.items():
        if "ObjectId" in meta["types"]:
            target = field[:-2] if field.endswith("Id") else None
            if target and target in entity_fields:
                lines.append(f"    {target} ||--o{{ {ent}: \"\"")

Path("schema_output.mmd").write_text("\n".join(lines))
print("Generated schema_output.mmd for", len(collections), "collections.")
