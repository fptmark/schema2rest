#!/usr/bin/env python3
"""es_extractor.py – Generates Mermaid diagram from Elasticsearch database"""
from __future__ import annotations

import sys
import json
import re
import logging
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, List, Set
from utilities.utils import load_system_config

from elasticsearch import Elasticsearch

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

# Email and URL regex (used from Mongo2Mmd)
EMAIL_RX = re.compile(r"^[a-zA-Z0-9._%-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
URL_RX = re.compile(r"^https?://[^\s]+$")
CANDIDATE_PATTERNS = {
    "email": (EMAIL_RX, "Bad email address format"),
    "url":   (URL_RX,   "Bad URL format"),
}

ENUM_MIN_FREQ   = 10       # each distinct value must appear ≥ 10×
ENUM_MAX_SIZE   = 15       # max enum cardinality
ENUM_COVERAGE   = 0.98     # enum must cover ≥ 98 % of non‑null rows
COMMON_THRESHOLD = 0.80    # field present in ≥ 80 % of collections
SAMPLE_SIZE      = 500     # docs to sample per collection

def infer_pattern(values: List[str]):
    for _key, (rx, msg) in CANDIDATE_PATTERNS.items():
        if all(rx.fullmatch(v) for v in values if isinstance(v, str)):
            return rx.pattern, msg
    return None

def infer_enum(values: List[Any]):
    cleaned = [v for v in values if v is not None]
    if len(cleaned) < 20:
        return None
    freq = Counter(cleaned)
    if len(freq) > 5 or any(c < ENUM_MIN_FREQ for c in freq.values()):
        return None
    if sum(freq.values()) / len(values) < ENUM_COVERAGE:
        return None
    return sorted(freq)

def es_to_dtype(types: Set[str]):
    """Convert Elasticsearch field types to our custom types"""
    type_mapping = {
        'text': 'String',
        'keyword': 'String',
        'long': 'Integer',
        'integer': 'Integer',
        'short': 'Integer',
        'float': 'Number',
        'double': 'Number',
        'boolean': 'Boolean',
        'date': 'ISODate',
        'object': 'JSON',
        'nested': 'JSON'
    }
    
    for es_type in types:
        mapped_type = type_mapping.get(es_type.lower())
        if mapped_type:
            return mapped_type
    return 'String'

def analyze_field_validation(values: List[Any], dtype: str):
    """Similar to Mongo2Mmd validation, adapted for Elasticsearch"""
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

def generate_mmd(config_path: str):
    config = load_system_config(config_path)
    client = Elasticsearch(config["es_uri"])
    log_level = config["log_level"].upper()
    
    # Set logger level based on config
    logger.setLevel(getattr(logging, log_level))
    console_handler.setLevel(getattr(logging, log_level))
    
    # Get indices
    collections = config.get("collections", [])
    if not collections:
        collections = [
            idx for idx in client.indices.get_alias().keys()
            if not idx.startswith(".")  # Exclude system indices
        ]
    
    logger.info(f"Found {len(collections)} collections in database")

    # Initialize data structures
    entity_fields: Dict[str, Dict[str, Any]] = {}
    entity_indexes: Dict[str, List[Dict]] = {}
    field_to_collections: Dict[str, Set[str]] = defaultdict(set)
    field_global_types: Dict[str, Set[str]] = defaultdict(set)

    # Collect metadata for each index
    for coll_name in collections:
        # Get mapping
        mapping = client.indices.get_mapping(index=coll_name)
        
        # Fetch sample documents
        search_body = {"size": SAMPLE_SIZE}
        search_results = client.search(index=coll_name, body=search_body)
        docs = search_results['hits']['hits']
        
        if not docs:
            continue

        per_field: Dict[str, Dict[str, Any]] = defaultdict(
            lambda: {"types": set(), "values": []}
        )

        # Process each document
        for doc in docs:
            source = doc['_source']
            for k, v in source.items():
                meta = per_field[k]
                meta["types"].add(type(v).__name__)
                if isinstance(v, (str, int, float, bool)) and len(meta["values"]) < SAMPLE_SIZE:
                    meta["values"].append(v)
        
        entity_fields[coll_name] = per_field

        # Collect field metadata
        for f, m in per_field.items():
            field_to_collections[f].add(coll_name)
            field_global_types[f].update(m["types"])

        # Extract index information
        collection_indexes = []
        # Note: Elasticsearch index retrieval differs from MongoDB
        collection_indexes.append({
            "fields": list(mapping[coll_name]['mappings']['properties'].keys()),
            "unique": False,  # Elasticsearch handles uniqueness differently
            "name": coll_name
        })
        
        entity_indexes[coll_name] = collection_indexes

    # Base Entity Detection
    non_empty_collections = [
        coll for coll in collections 
        if coll in entity_fields and entity_fields[coll]
    ]
    num_col = len(non_empty_collections)
    base_fields: List[tuple[str, str]] = []
    
    # Detect base fields
    for f, collset in field_to_collections.items():
        valid_collections = [
            coll for coll in collset 
            if coll in non_empty_collections
        ]
        coverage = len(valid_collections) / num_col
        type_diversity = len(field_global_types[f])
        
        if (coverage >= COMMON_THRESHOLD and type_diversity == 1):
            base_fields.append((f, next(iter(field_global_types[f]))))

    # Generate Mermaid Diagram
    lines: List[str] = ["erDiagram"]

    # BaseEntity
    if base_fields:
        lines.append("    BaseEntity {")
        for fname, t in base_fields:
            lines.append(f"        {es_to_dtype({t})} {fname}")
        lines.append("")
        lines.append("        %% @abstract")
        lines.append("    }\n")

    # Concrete Entities
    for ent, fields in entity_fields.items():
        lines.append(f"    {ent} {{")

        # Field lines
        base_field_names = {f for f, _ in base_fields}
        for field, meta in fields.items():
            if field in base_field_names:
                continue
            
            dtype = es_to_dtype(meta["types"])
            validation_str = ""
            
            # Validation rules
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

        # Decorators
        decorators: List[str] = []
        if base_fields:
            decorators.append("        %% @include BaseEntity")
        
        for idx in entity_indexes.get(ent, []):
            if idx.get('unique', False):
                fields = idx.get('fields', [])
                if len(fields) == 1:
                    decorators.append(f"        %% @unique {fields[0]}")
                elif len(fields) > 1:
                    decorators.append(f"        %% @unique {' + '.join(fields)}")

        if decorators:
            lines.append("")
            lines.extend(decorators)

        lines.append("    }\n")

    # Relationships
    for ent, fields in entity_fields.items():
        for field, meta in fields.items():
            # Detect relationships via ObjectId-like fields
            if "ObjectId" in meta["types"] or field.endswith("Id"):
                target = field[:-2] if field.endswith("Id") else None
                if target and target in entity_fields:
                    lines.append(f"    {target} ||--o{{ {ent}: \"\"")

    # Write Mermaid diagram
    Path("es_schema.mmd").write_text("\n".join(lines))
    print("Generated es_schema.mmd for", len(collections), "collections.")

if __name__ == "__main__":
    generate_mmd("config.json" if len(sys.argv) < 2 else sys.argv[1])