import json
from pathlib import Path
from typing import Dict, Any, Optional, TypeVar, Type
from pydantic import BaseModel
from beanie import Document
import logging
from datetime import datetime, timezone
import re


# Path to the configuration file

T = TypeVar('T')

def load_settings(config_file: Path | None, required: bool = True) -> Dict[str, Any]:
    try:
        if config_file:
            with open(config_file, 'r') as config_handle:
                return json.load(config_handle)
    except Exception as e:
        if required:
            logging.error(f"Error loading config file {config_file}: {e}")
        return {}

    return {}


def deep_merge_dicts(dest, override):
    for key, value in override.items():
        if (
            key in dest
            and isinstance(dest[key], dict)
            and isinstance(value, dict)
        ):
            deep_merge_dicts(dest[key], value)
        else:
            dest[key] = value


def get_metadata(entity: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
    """Get metadata for a model with proper type hints"""
    overrides = load_settings(Path('overrides.json'), False) or {}
    # name = metadata.get('fields', '')
    entity_cfg = overrides.get(entity, {})
    if entity_cfg:
        deep_merge_dicts(metadata, entity_cfg)
    return metadata


def format_datetime(dt: Optional[datetime] = None) -> str:
    """Format a datetime object to ISO format"""
    if dt is None:
        dt = datetime.now(timezone.utc)
    return dt.isoformat()

def parse_datetime(dt_str: str) -> datetime:
    """Parse an ISO format datetime string"""
    return datetime.fromisoformat(dt_str)

def validate_id(id: str) -> bool:
    """Validate if a string is a valid ID format"""
    return bool(id and isinstance(id, str) and len(id) > 0)

def sanitize_field_name(field: str) -> str:
    """Sanitize a field name for database operations"""
    return field.strip().replace('.', '_')
