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
    async def get_all(cls) -> Dict[str, Any]:
        try:
            get_validations, unique_validations = Config.validations(True)
            unique_constraints = cls._metadata.get('uniques', []) if unique_validations else []
            
            raw_docs, warnings, total_count = await DatabaseFactory.get_all("{{EntityLower}}", unique_constraints)
            
            {{EntityLower}}s = []
            
            # Conditional validation - validate AFTER read if requested
            if get_validations:
                for doc in raw_docs:
                    try:
                        {{EntityLower}}s.append(cls.model_validate(doc))
                    except PydanticValidationError as e:
                        # Convert Pydantic errors to notifications
                        entity_id = doc.get('id')
                        if not entity_id:
                            notify_warning("Document missing ID field", NotificationType.DATABASE, entity={{Entity}})
                            entity_id = "missing"
  
                        for error in e.errors():
                            field_name = str(error['loc'][-1])
                            notify_warning(
                                message=error['msg'],
                                type=NotificationType.VALIDATION,
                                entity="{{Entity}}",
                                field_name=field_name,
                                value=error.get('input'),
                                operation="get_all",
                                entity_id=entity_id
                            )

                        # Create instance without validation for failed docs
                        {{EntityLower}}s.append(cls.model_construct(**doc))
            else:
                {{EntityLower}}s = [cls.model_construct(**doc) for doc in raw_docs]  # NO validation  
            
            # Add database warnings
            for warning in warnings:
                notify_warning(warning, NotificationType.DATABASE)
            
            # Convert models to dictionaries for FastAPI response validation
            {{EntityLower}}_data = []
            for {{EntityLower}} in {{EntityLower}}s:
                with python_warnings.catch_warnings(record=True) as caught_warnings:
                    python_warnings.simplefilter("always")
                    data_dict = {{EntityLower}}.model_dump()
                    {{EntityLower}}_data.append(data_dict)
                    
                    # Add any serialization warnings as notifications
                    if caught_warnings:
                        entity_id = data_dict.get('id')
                        if not entity_id:
                            notify_warning("Document missing ID field", NotificationType.DATABASE)
                            entity_id = "missing"

                        datetime_field_names = []
                        
                        # Use the model's metadata to find datetime fields
                        for field_name, field_meta in cls._metadata.get('fields', {}).items():
                            if field_meta.get('type') == 'ISODate':
                                if field_name in data_dict and isinstance(data_dict[field_name], str):
                                    datetime_field_names.append(field_name)
                        
                        if datetime_field_names:
                            field_list = ', '.join(datetime_field_names)
                            notify_warning(f"{field_list} datetime serialization warnings", NotificationType.VALIDATION, entity="{{Entity}}", entity_id=entity_id)
                        else:
                            # Fallback for non-datetime warnings
                            warning_count = len(caught_warnings)
                            notify_warning(f"User {entity_id}: {warning_count} serialization warnings", NotificationType.VALIDATION, entity="{{Entity}}")
 
            return {"data": {{EntityLower}}_data}
            
        except Exception as e:
            raise DatabaseError(str(e), "{{Entity}}", "get_all")


    @classmethod
    async def get(cls, id: str) -> tuple[Self, List[str]]:
        try:
            get_validations, unique_validations = Config.validations(False)
            unique_constraints = cls._metadata.get('uniques', []) if unique_validations else []
            
            raw_doc, warnings = await DatabaseFactory.get_by_id("{{EntityLower}}", str(id), unique_constraints)
            if not raw_doc:
                raise NotFoundError("{{Entity}}", id)
            
            # Database warnings are now handled by DatabaseFactory
            
            # Conditional validation - validate AFTER read if requested
            if get_validations:
                try:
                    return cls.model_validate(raw_doc), warnings  # WITH validation
                except PydanticValidationError as e:
                    # Convert validation errors to notifications
                    entity_id = raw_doc.get('id')
                    if not entity_id:
                        notify_warning("Document missing ID field", NotificationType.DATABASE)
                        entity_id = "missing"
                    for error in e.errors():
                        field_name = str(error['loc'][-1])
                        notify_warning(
                            message=f"{{Entity}} {entity_id}: {field_name} validation failed - {error['msg']}",
                            type=NotificationType.VALIDATION,
                            entity="{{Entity}}",
                            field_name=field_name,
                            value=error.get('input'),
                            operation="get",
                            entity_id=entity_id
                        )
                    return cls.model_construct(**raw_doc), warnings  # Fallback to no validation
            else:
                return cls.model_construct(**raw_doc), warnings  # NO validation
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
            except PydanticValidationError as e:
                # Convert to notifications and ValidationError format
                if len(entity_id) == 0:
                    notify_warning("User instance missing ID during save", NotificationType.DATABASE)
                    entity_id = "missing"

                for err in e.errors():
                    field_name = str(err["loc"][-1])
                    notify_warning(
                        message=f"{{Entity}} {entity_id}: {field_name} validation failed - {err['msg']}",
                        type=NotificationType.VALIDATION,
                        entity="{{Entity}}",
                        field_name=field_name,
                        value=err.get("input"),
                        operation="save"
                    )
                failures = [ValidationFailure(field_name=str(err["loc"][-1]), message=err["msg"], value=err.get("input")) for err in e.errors()]
                raise ValidationError(message="Validation failed before save", entity="{{Entity}}", invalid_fields=failures)
            
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
