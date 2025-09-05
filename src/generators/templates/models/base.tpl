from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Self, ClassVar, Tuple
from enum import Enum
from pydantic import BaseModel, Field, ConfigDict, field_validator, ValidationError as PydanticValidationError, BeforeValidator, Json
from app.db import DatabaseFactory
from app.config import Config
from app.services.metadata import MetadataService
import app.models.utils as utils

{{EnumClasses}}

class {{Entity}}(BaseModel):
    id: str | None = Field(default=None)
    {{BaseFields}}
    {{AutoFields}}

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
        validate = Config.validation(True)
        
        # Get filtered data from database - RequestContext provides the parameters
        data_records, total_count = await DatabaseFactory.get_all("{{Entity}}", sort, filter, page, pageSize)
        
        #if data_records:
        for data in data_records:
            # Always run Pydantic validation (required fields, types, ranges)
            utils.validate_model(cls, data, "{{Entity}}")
            
            if validate:
                unique_constraints = cls._metadata.get('uniques', [])
                await utils.validate_uniques("{{Entity}}", data, unique_constraints, None)
            
            # Populate view data if requested and validate fks
            await utils.process_fks("{{Entity}}", data, validate, view_spec)
        
        return data_records, total_count

    @classmethod
    async def get(cls, id: str, view_spec: Dict[str, Any]) -> Tuple[Dict[str, Any], int]:
        validate = Config.validation(False)
        
        data, record_count = await DatabaseFactory.get_by_id(str(id), "{{Entity}}")
        if data:
            
            # Always run Pydantic validation (required fields, types, ranges)
            utils.validate_model(cls, data, "{{Entity}}")
            
            if validate:
                unique_constraints = cls._metadata.get('uniques', [])
                await utils.validate_uniques("{{Entity}}", data, unique_constraints, None)
            
            # Populate view data if requested and validate fks
            await utils.process_fks("{{Entity}}", data, validate, view_spec)
        
        return data, record_count


    @classmethod
    async def create(cls, data: Dict[str, Any], validate: bool = True) -> Tuple[Dict[str, Any], int]:
        {{AutoUpdateLines}}
        
        if validate:
            validated_instance = utils.validate_model(cls, data, "{{Entity}}")
            data = validated_instance.model_dump(mode='python')
            
            unique_constraints = cls._metadata.get('uniques', [])
            await utils.validate_uniques("{{Entity}}", data, unique_constraints, None)

            # Validate fks
            await utils.process_fks("{{Entity}}", data, True)
        
        # Create new document
        return await DatabaseFactory.create("{{Entity}}", data)

    @classmethod
    async def update(cls, data: Dict[str, Any]) -> Tuple[Dict[str, Any], int]:
        {{AutoUpdateLines}}

        validated_instance = utils.validate_model(cls, data, "{{Entity}}")
        data = validated_instance.model_dump(mode='python')
        
        unique_constraints = cls._metadata.get('uniques', [])
        await utils.validate_uniques("{{Entity}}", data, unique_constraints, data['id'])

        # Validate fks
        await utils.process_fks("{{Entity}}", data, True)
    
        # Update existing document
        return await DatabaseFactory.update("{{Entity}}", data)

    @classmethod
    async def delete(cls, id: str) -> Tuple[Dict[str, Any], int]:
        return await DatabaseFactory.delete("{{Entity}}", id)