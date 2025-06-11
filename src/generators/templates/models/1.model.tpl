from collections.abc import Sequence
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Self, ClassVar
from pydantic import BaseModel, Field, ConfigDict, field_validator
import re
from app.db import DatabaseFactory
import app.utils as helpers
from app.errors import ValidationError, ValidationFailure, NotFoundError, DuplicateError, DatabaseError


class UniqueValidationError(Exception):
    def __init__(self, fields, query):
        self.fields = fields
        self.query = query

    def __str__(self):
        return f"Unique constraint violation for fields {self.fields}: {self.query}"


class {{Entity}}(BaseModel):
    id: Optional[str] = Field(default=None, alias="_id")
    {{BaseFields}}
    {{AutoFields}}

    _metadata: ClassVar[Dict[str, Any]] = {{Metadata}}

    class Settings:
        name = "{{EntityLower}}"

    model_config = ConfigDict(
        populate_by_name=True,
    )

    {{Validators}}

    @classmethod
    def get_metadata(cls) -> Dict[str, Any]:
        return helpers.get_metadata(cls._metadata)

    @classmethod
    async def find_all(cls) -> Sequence[Self]:
        try:
            return await DatabaseFactory.find_all("{{EntityLower}}", cls)
        except Exception as e:
            raise DatabaseError(str(e), "{{Entity}}", "find_all")

    @classmethod
    def find(cls):
        class FindAdapter:
            @staticmethod
            async def to_list():
                return await cls.find_all()

        return FindAdapter()

    @classmethod
    async def get(cls, id: str) -> Self:
        try:
            {{EntityLower}} = await DatabaseFactory.get_by_id("{{EntityLower}}", str(id), cls)
            if not {{EntityLower}}:
                raise NotFoundError("{{Entity}}", id)
            return {{EntityLower}}
        except NotFoundError:
            raise
        except Exception as e:
            raise DatabaseError(str(e), "{{Entity}}", "get")

    async def save(self, doc_id: Optional[str] = None) -> Self:
        try:
            {{AutoUpdateLines}}
            if doc_id:
                self.id = doc_id

            data = self.model_dump(exclude={"id"})
            
            # Get unique constraints from metadata
            unique_constraints = self._metadata.get('uniques', [])
            
            # Save document with unique constraints
            result = await DatabaseFactory.save_document("{{EntityLower}}", self.id, data, unique_constraints)
            
            # Update ID from result
            if not self.id and result and isinstance(result, dict) and result.get(DatabaseFactory.get_id_field()):
                self.id = result[DatabaseFactory.get_id_field()]

            return self
        except ValidationError:
            # Re-raise validation errors directly
            raise
        except Exception as e:
            raise DatabaseError(str(e), "{{Entity}}", "save")
            
    async def delete(self) -> bool:
        if not self.id:
            raise ValidationError(
                message="Cannot delete {{EntityLower}} without ID",
                entity="{{Entity}}",
                invalid_fields=[ValidationFailure("id", "ID is required for deletion", None)]
            )
        try:
            result = await DatabaseFactory.delete_document("{{EntityLower}}", self.id)
            if not result:
                raise NotFoundError("{{Entity}}", self.id)
            return True
        except NotFoundError:
            raise
        except Exception as e:
            raise DatabaseError(str(e), "{{Entity}}", "delete")
