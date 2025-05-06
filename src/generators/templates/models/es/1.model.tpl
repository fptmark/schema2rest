from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Self, ClassVar
from pydantic import BaseModel, Field, ConfigDict, field_validator
from beanie import PydanticObjectId
from elasticsearch import NotFoundError
import re
from db import Database
import app.utils as helpers

class {{Entity}}BaseModel(BaseModel):
    __index__ = "{{EntityLower}}"
    __unique__ = {{UniqueList}}
    __mappings__ = {{MappingsDict}}

    model_config = ConfigDict(populate_by_name=True)

    {{BaseFields}}

    {{AutoFields}}

    {{SaveFunction}}

class {{Entity}}({{Entity}}BaseModel):
    id: Optional[str] = Field(default=None, alias="_id")
 
    __ui_metadata__: ClassVar[Dict[str, Any]] = {{Metadata}}

    @classmethod
    def get_metadata(cls) -> Dict[str, Any]:
        return helpers.get_metadata(cls.__ui_metadata__)
 
    @classmethod
    def _from_es_hit(cls, hit: dict) -> "{{Entity}}":
        data = hit["_source"]
        data["id"] = hit["_id"]
        return cls(**data)

    @classmethod
    async def get(cls, item_id: str):
        es = Database.get_es_client()
        if not es:
            raise RuntimeError("Elasticsearch client not initialized — did you forget to call Database.init()?")
        try:
            result = await es.get(index=cls.__index__, id=item_id)
            return cls._from_es_hit(result.body)
        except Exception:
            return None

    @classmethod
    async def find_all(cls) -> List[Self]:
        es = Database.get_es_client()
        if not es:
            raise RuntimeError("Elasticsearch client not initialized — did you forget to call Database.init()?")
        try:
            result = await es.search(index=cls.__index__, query={"match_all": {}}, size=1000)
            return [cls._from_es_hit(hit) for hit in result["hits"]["hits"]] #type: ignore[return-value]
        except NotFoundError:
            return []  # Return empty list if index doesn't exist