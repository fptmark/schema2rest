from collections.abc import Sequence
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Self, ClassVar, Union, Annotated, Literal
from enum import Enum
from pydantic import BaseModel, Field, ConfigDict, field_validator, ValidationError as PydanticValidationError, BeforeValidator, Json
from pydantic_core import core_schema
import warnings as python_warnings
from app.db import DatabaseFactory
import app.utils as helpers
from app.config import Config
from app.errors import ValidationError, ValidationFailure, NotFoundError, DuplicateError, DatabaseError
from app.notification import notify_warning, NotificationType
from app.models.list_params import ListParams
import app.models.utils as utils

{{EnumClasses}}

class UniqueValidationError(Exception):
    def __init__(self, fields, query):
        self.fields = fields
        self.query = query

    def __str__(self):
        return f"Unique constraint violation for fields {self.fields}: {self.query}"


class {{Entity}}(BaseModel):
    id: str | None = Field(default=None)
    {{BaseFields}}
    {{AutoFields}}

    model_config = ConfigDict(
        json_encoders={
            datetime: lambda v: v.isoformat() + 'Z' if v else None  # Always UTC with Z suffix
        }
    )

    _metadata: ClassVar[Dict[str, Any]] = {{Metadata}}

    class Settings:
        name = "{{EntityLower}}"

    model_config = ConfigDict(from_attributes=True, validate_by_name=True, use_enum_values=True)

    @classmethod
    def get_metadata(cls) -> Dict[str, Any]:
        return helpers.get_metadata("{{Entity}}", cls._metadata)


    @classmethod
    async def get_list(cls, list_params: Optional[ListParams] = None, view_spec: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Get paginated, sorted, and filtered list of entity."""
        try:
            fk_validations, unique_validations = Config.validations(True)
            unique_constraints = cls._metadata.get('uniques', []) if unique_validations else []
            
            # Use default pagination if none provided (same as DatabaseFactory does)
            if list_params is None:
                list_params = ListParams(page=1, page_size=100)
            
            # Get filtered data from database
            raw_docs, warnings, total_count = await DatabaseFactory.get_list("{{EntityLower}}", unique_constraints, list_params, cls._metadata)
            
            # Use common processing
            {{EntityLower}}_data = utils.process_raw_results(cls, "{{Entity}}", raw_docs, warnings)
            
            # Process FK fields if needed
            if view_spec or fk_validations:
                for {{EntityLower}}_dict in {{EntityLower}}_data:
                    await utils.process_entity_fks({{EntityLower}}_dict, view_spec, "{{Entity}}", cls, fk_validations)

            return {
                "data": {{EntityLower}}_data,
                "page_size": list_params.page_size,
                "total_count": total_count,
                "page": list_params.page,
                "total_pages": (total_count + list_params.page_size - 1) // list_params.page_size,
                "pagination": {
                    "page": list_params.page,
                    "per_page": list_params.page_size,
                    "total": total_count,
                    "total_pages": (total_count + list_params.page_size - 1) // list_params.page_size,
                    "has_next": list_params.page < (total_count + list_params.page_size - 1) // list_params.page_size,
                    "has_prev": list_params.page > 1
                }
            }
            
        except Exception as e:
            raise DatabaseError(str(e), "{{Entity}}", "get_list")


    @classmethod
    async def get(cls, id: str, view_spec: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        try:
            fk_validations, unique_validations = Config.validations(False)
            unique_constraints = cls._metadata.get('uniques', []) if unique_validations else []
            
            raw_doc, warnings = await DatabaseFactory.get_by_id("{{EntityLower}}", str(id), unique_constraints)
            if not raw_doc:
                raise NotFoundError("{{Entity}}", id)
            
            # Database warnings are now handled by DatabaseFactory
            
            # Step 1: Use Pydantic validation with notification conversion
            {{EntityLower}}_instance = utils.validate_with_notifications(cls, raw_doc, "{{Entity}}")
            
            # Step 2: Get validated dict and process FK fields if needed
            {{EntityLower}}_dict = {{EntityLower}}_instance.model_dump()
            if view_spec or fk_validations:
                await utils.process_entity_fks({{EntityLower}}_dict, view_spec, "{{Entity}}", cls, fk_validations)
            
            return {
                "data": {{EntityLower}}_dict,
                "warnings": warnings
            }
        except NotFoundError:
            raise
        except DatabaseError:
            raise
        except Exception as e:
            raise DatabaseError(str(e), "{{Entity}}", "get") 

    async def save(self, entity_id: str = '') -> tuple[Self, List[str]]:
        try:
            _, unique_validations = Config.validations(True)
            unique_constraints = self._metadata.get('uniques', []) if unique_validations else []

            # update uses the id
            if len(entity_id) > 0:
                self.id = entity_id
            
            {{AutoUpdateLines}}
            
            # VALIDATE the instance BEFORE saving to prevent bad data in DB
            try:
                # This validates all fields and raises PydanticValidationError if invalid
                validated_instance = self.__class__.model_validate(self.model_dump())
                # Use the validated data for save
                data = validated_instance.model_dump()
                                
                # Validate ObjectId references exist
                await utils.validate_objectid_references("{{Entity}}", data, self._metadata)
            except PydanticValidationError as e:
                # Convert to notifications and ValidationError format
                if len(entity_id) == 0:
                    notify_warning("{{Entity}} instance missing ID during save", NotificationType.DATABASE)
                    entity_id = "missing"

                for error in e.errors():
                    field_name = str(error["loc"][-1])
                    notify_warning(
                        message=error['msg'],
                        type=NotificationType.VALIDATION,
                        entity="{{Entity}}",
                        field_name=field_name,
                        value=error.get("input"),
                        operation="save"
                    )
                failures = [ValidationFailure(field_name=str(error["loc"][-1]), message=error["msg"], value=error.get("input")) for error in e.errors()]
                raise ValidationError(message=error['msg'], entity="{{Entity}}", invalid_fields=failures)
            
            # Save document with unique constraints - pass complete data
            result, warnings = await DatabaseFactory.save_document("{{EntityLower}}", data, unique_constraints)

            # Update ID from result
            if not self.id and result and isinstance(result, dict):
                extracted_id = result.get('id')
                if extracted_id:
                    self.id = extracted_id

            return self, warnings
        except ValidationError:
            # Re-raise validation errors directly
            raise
        except Exception as e:
            raise DatabaseError(str(e), "{{Entity}}", "save")
 
    @classmethod
    async def delete(cls, {{EntityLower}}_id: str) -> tuple[bool, List[str]]:
        try:
            result = await DatabaseFactory.delete_document("{{EntityLower}}", {{EntityLower}}_id)
            if not result:
                raise NotFoundError("{{Entity}}", {{EntityLower}}_id)
            return True, []
        except NotFoundError:
            raise
        except Exception as e:
            raise DatabaseError(str(e), "{{Entity}}", "delete")
