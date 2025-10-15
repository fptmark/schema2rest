from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Self, ClassVar, Tuple
from enum import Enum
from pydantic import BaseModel, Field, ConfigDict, field_validator, ValidationError as PydanticValidationError, BeforeValidator, Json
from app.db import DatabaseFactory
from app.services.metadata import MetadataService

{{EnumClasses}}

{{CreateClass}}

{{UpdateClass}}


class {{Entity}}(BaseModel):
    id: str | None = Field(default=None)
    {{BaseFields}}
    {{AutoGenerateFields}}
    {{AutoUpdateFields}}

    model_config = ConfigDict()

    _metadata: ClassVar[Dict[str, Any]] = {{Metadata}}

    class Settings:
        name = "{{EntityLower}}"

    model_config = ConfigDict(from_attributes=True, validate_by_name=True, use_enum_values=True)

    @classmethod
    def get_metadata(cls) -> Dict[str, Any]:
        return MetadataService.get("{{Entity}}")

    @classmethod
    async def get_all(cls,
                      sort: List[Tuple[str, str]], 
                      filter: Optional[Dict[str, Any]], 
                      page: int, 
                      pageSize: int, 
                      view_spec: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], int]:
        "Get paginated, sorted, and filtered list of entity." 
        
        db = DatabaseFactory.get_instance()
        return await db.documents.get_all("{{Entity}}", sort, filter, page, pageSize, view_spec)
        
    @classmethod
    async def get(cls, id: str, view_spec: Dict[str, Any], top_level: bool = True) -> Tuple[Dict[str, Any], int, Optional[BaseException]]:
        db = DatabaseFactory.get_instance()
        return await db.documents.get("{{Entity}}", id, view_spec, top_level)

    @classmethod
    async def create(cls, data: {{Entity}}Create, validate: bool = True) -> Tuple[Dict[str, Any], int]:
        db = DatabaseFactory.get_instance()
        return await db.documents.create("{{Entity}}", data.model_dump())

    @classmethod
    async def update(cls, id, data: {{Entity}}Update) -> Tuple[Dict[str, Any], int]:
        db = DatabaseFactory.get_instance()
        return await db.documents.update("{{Entity}}", id, data.model_dump())

    @classmethod
    async def delete(cls, id: str) -> Tuple[Dict[str, Any], int]:
        db = DatabaseFactory.get_instance()
        return await db.documents.delete("{{Entity}}", id)