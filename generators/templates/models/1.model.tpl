from pathlib import Path
from beanie import Document, PydanticObjectId, before_event, Insert
from pydantic import BaseModel, Field, validator
from typing import Optional, List, Dict, Any, ClassVar
from datetime import datetime, timezone
import app.utilities.utils as helpers
import logging

class UniqueValidationError(Exception):
    def __init__(self, fields, query):
        self.fields = fields
        self.query = query
    def __str__(self):
        return f"Unique constraint violation for fields {self.fields}: {self.query}"

class ${Entity}(Document):
    ${RegularFieldDefs}
    ${AutoFieldDefs}

    __ui_metadata__: ClassVar[Dict[str, Any]] = ${Metadata}

    class Settings:
        name = "${entity_lower}"

    @classmethod
    def get_metadata(cls) -> Dict[str, Any]:
        return helpers.get_metadata(cls.__ui_metadata__)

    ${SaveMethod}
