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

import sys
import json
import re
import logging
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, List, Set
from utilities.utils import load_system_config

from pymongo import MongoClient

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)  # Default to INFO level

# Create console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# Create formatter
formatter = logging.Formatter('%(message)s')
console_handler.setFormatter(formatter)

# Add handler to logger
logger.addHandler(console_handler)

def set_log_level(level: str):
    """Set the logging level."""
    level = level.upper()
    if level == 'DEBUG':
        logger.setLevel(logging.DEBUG)
        console_handler.setLevel(logging.DEBUG)
    elif level == 'INFO':
        logger.setLevel(logging.INFO)
        console_handler.setLevel(logging.INFO)
    elif level == 'WARNING':
        logger.setLevel(logging.WARNING)
        console_handler.setLevel(logging.WARNING)
    elif level == 'ERROR':
        logger.setLevel(logging.ERROR)
        console_handler.setLevel(logging.ERROR)

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
    """
    Infer regex pattern for a set of string values
    
    Args:
        values (List[str]): List of string values to analyze
    
    Returns:
        Tuple[str, str] or None: (pattern, error_message) if a pattern is found, else None
    """
    for _key, (rx, msg) in CANDIDATE_PATTERNS.items():
        if all(rx.fullmatch(v) for v in values if isinstance(v, str)):
            return rx.pattern, msg
    return None


def infer_enum(values: List[Any]):
    """
    Infer enum values from a list of values
    
    Args:
        values (List[Any]): List of values to analyze for enum potential
    
    Returns:
        List or None: Sorted list of enum values if criteria are met, else None
    """
    # Filter out None values
    cleaned = [v for v in values if v is not None]
    
    # Require at least 20 records
    if len(cleaned) < 20:
        return None
    
    # Count frequency of values
    freq = Counter(cleaned)
    
    # Check enum constraints
    # 1. Unique values <= 5
    # 2. Each value appears at least 10 times
    # 3. Enum covers at least 98% of non-null rows
    if (len(freq) > 5 or 
        any(c < ENUM_MIN_FREQ for c in freq.values()) or 
        sum(freq.values()) / len(values) < ENUM_COVERAGE):
        return None
    
    return sorted(freq)


def analyze_field_validation(values: List[Any], dtype: str):
    """
    Comprehensive field validation analysis
    
    Args:
        values (List[Any]): List of field values
        dtype (str): Data type of the field
    
    Returns:
        Dict: Validation rules for the field
    """
    if not values:
        return None
    
    validation_rules = {}
    
    # String specific validations
    if dtype == 'String':
        # Pattern detection
        pattern_info = infer_pattern(values)
        if pattern_info:
            pat_src, pat_msg = pattern_info
            validation_rules['pattern'] = {
                'regex': pat_src.replace("\\", "\\\\"),
                'message': pat_msg
            }
        
        # Enum detection
        enum_vals = infer_enum(values)
        if enum_vals:
            validation_rules['enum'] = list(enum_vals)
        
        # String length
        if len(values) >= 100:
            lengths = [len(str(v)) for v in values]
            validation_rules['length'] = {
                'min': min(lengths),
                'max': max(lengths)
            }
    
    # Numeric validations
    elif dtype in ['Integer', 'Number']:
        if len(values) >= 100:
            validation_rules['range'] = {
                'min': min(values),
                'max': max(values)
            }
    
    return validation_rules


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
def generate_mmd(config_path: str):
    config = load_system_config(config_path)
    client = MongoClient(config["db_uri"])
    log_level = config["log_level"].upper()
    
    # Set logger level based on config
    if log_level == 'DEBUG':
        logger.setLevel(logging.DEBUG)
        console_handler.setLevel(logging.DEBUG)
    elif log_level == 'INFO':
        logger.setLevel(logging.INFO)
        console_handler.setLevel(logging.INFO)
    elif log_level == 'WARNING':
        logger.setLevel(logging.WARNING)
        console_handler.setLevel(logging.WARNING)
    elif log_level == 'ERROR':
        logger.setLevel(logging.ERROR)
        console_handler.setLevel(logging.ERROR)
    
    db = client[config["db_name"]]

    collections = config.get("collections", [])
    if len(collections) == 0:
        collections = [
            c for c in db.list_collection_names(filter={"type": "collection"})
            if not c.startswith("system.") and c not in {"fs.files", "fs.chunks"}
        ]
    
    logger.info(f"Found {len(collections)} collections in database {config['db_name']}")

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
        
        # Additional validation rule analysis
        for field, meta in per_field.items():
            if meta["values"]:
                dtype = bson_to_dtype(meta["types"])
                validation_rules = analyze_field_validation(meta["values"], dtype)
                if validation_rules:
                    meta["validation"] = validation_rules

        entity_fields[coll_name] = per_field

        for f, m in per_field.items():
            field_to_collections[f].add(coll_name)
            field_global_types[f].update(m["types"])

        # Extract all indexes, tracking their properties
        collection_indexes = []
        for idx in list(coll.list_indexes()):
            # Extract fields in the index
            idx_fields = list(idx.get('key', {}).keys())
            
            # Determine index properties
            idx_props = {
                "fields": idx_fields,
                "unique": idx.get("unique", False),
                "sparse": idx.get("sparse", False),
                "name": idx.get("name", "")
            }
            
            collection_indexes.append(idx_props)
        
        # Store indexes with their properties
        entity_indexes[coll_name] = collection_indexes
        
        # Log index information for debugging
        logger.debug(f"Indexes for collection {coll_name}:")
        for idx in collection_indexes:
            logger.debug(f"  - Fields: {idx['fields']}")
            logger.debug(f"    Unique: {idx['unique']}")
            logger.debug(f"    Sparse: {idx['sparse']}")

    # ───────────────────────────────────────── derive BaseEntity candidates ───
    # Filter out collections with no records
    non_empty_collections = [coll for coll in collections if coll in entity_fields and entity_fields[coll]]
    num_col = len(non_empty_collections)
    base_fields: List[tuple[str, str]] = []
    
    # Base Entity Candidate Detection
    logger.debug(f"Total Collections: {len(collections)}")
    logger.debug(f"Non-empty Collections: {num_col}")
    logger.debug(f"Common Threshold: {COMMON_THRESHOLD}")
    
    logger.debug("\nField Analysis:")
    for f, collset in field_to_collections.items():
        # Only consider collections that have records
        valid_collections = [coll for coll in collset if coll in non_empty_collections]
        coverage = len(valid_collections) / num_col
        type_diversity = len(field_global_types[f])
        
        logger.debug(f"  Field '{f}':") 
        logger.debug(f"    - Present in Collections: {len(valid_collections)}/{num_col} (Coverage: {coverage:.2%})")
        logger.debug(f"    - Collections: {valid_collections}")
        logger.debug(f"    - Type Diversity: {type_diversity}")
        logger.debug(f"    - Types: {field_global_types[f]}")
        
        # Detailed condition breakdown
        if coverage < COMMON_THRESHOLD:
            logger.debug(f"    - SKIPPED: Insufficient coverage (< {COMMON_THRESHOLD})")
        elif type_diversity > 1:
            logger.debug("    - SKIPPED: Multiple type diversity")
        else:
            logger.debug(f"    - SELECTED as base field!")
            base_fields.append((f, next(iter(field_global_types[f]))))
    
    logger.info(f"Total base fields detected: {len(base_fields)}")
    
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
            # Use validation rules from metadata if available
            validation_str = ""
            validation = meta.get("validation", {})
            
            if validation:
                validate_parts = []
                if 'pattern' in validation:
                    pat = validation['pattern']
                    validate_parts.append(
                        f'pattern: {{ regex: "{pat["regex"]}", message: "{pat["message"]}" }}'
                    )
                
                if 'enum' in validation:
                    validate_parts.append(
                        f'enum: {{ {", ".join(map(str, validation["enum"]))} }}'
                    )
                
                if 'length' in validation:
                    length = validation['length']
                    validate_parts.append(
                        f'length: {{ min: {length["min"]}, max: {length["max"]} }}'
                    )
                
                if 'range' in validation:
                    range_val = validation['range']
                    validate_parts.append(
                        f'range: {{ min: {range_val["min"]}, max: {range_val["max"]} }}'
                    )
                
                validation_str = f" %% @validate {{ {', '.join(validate_parts)} }}" if validate_parts else ""
            
            lines.append(f"        {dtype} {field}{validation_str}")

        # ---- decorators (include + unique) ----
        decorators: List[str] = []
        if base_fields:
            decorators.append("        %% @include BaseEntity")
        
        # Add unique indexes as decorators
        for idx in entity_indexes.get(ent, []):
            if isinstance(idx, dict) and idx.get('unique', False):
                # For single-field unique indexes
                fields = idx.get('fields', [])
                if len(fields) == 1:
                    decorators.append(f"        %% @unique {fields[0]}")
                # For multi-field unique indexes
                elif len(fields) > 1:
                    decorators.append(f"        %% @unique {' + '.join(fields)}")

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

    Path("mongo_schema.mmd").write_text("\n".join(lines))
    print("Generated mongo_schema.mmd for", len(collections), "collections.")

if __name__ == "__main__":
    generate_mmd("config.json" if len(sys.argv) < 2 else sys.argv[1])
