from collections.abc import Sequence
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Self, ClassVar
from pydantic import BaseModel, Field, ConfigDict, field_validator, ValidationError as PydanticValidationError
import re
import logging
from app.db import DatabaseFactory
import app.utils as helpers
from app.config import Config
from app.errors import ValidationError, ValidationFailure, NotFoundError, DuplicateError, DatabaseError
from app.notification import notify_validation_error, NotificationType


class UniqueValidationError(Exception):
    def __init__(self, fields, query):
        self.fields = fields
        self.query = query

    def __str__(self):
        return f"Unique constraint violation for fields {self.fields}: {self.query}"


class {{Entity}}(BaseModel):
    {{BaseFields}}
    {{AutoFields}}

    _metadata: ClassVar[Dict[str, Any]] = {{Metadata}}

    class Settings:
        name = "{{EntityLower}}"

    model_config = ConfigDict(from_attributes=True, validate_by_name=True)

    def model_dump(self, **kwargs) -> Dict[str, Any]:
        """Override model_dump to normalize ID field to 'id' for API responses"""
        data = super().model_dump(**kwargs)
        # Normalize database-specific _id to generic id
        if "_id" in data:
            data["id"] = data.pop("_id")
        return data

    {{Validators}}

    @classmethod
    def get_metadata(cls) -> Dict[str, Any]:
        return helpers.get_metadata(cls._metadata)

    @classmethod
    async def get_all(cls) -> tuple[Sequence[Self], List[ValidationError]]:
        try:
            get_validations, unique_validations = Config.validations(True)
            unique_constraints = cls._metadata.get('uniques', []) if unique_validations else []
            
            raw_docs, warnings = await DatabaseFactory.get_all("{{EntityLower}}", unique_constraints)
            
            {{EntityLower}}s = []
            validation_errors = []
            
            # Conditional validation - validate AFTER read if requested
            if get_validations:
                for doc in raw_docs:
                    try:
                        {{EntityLower}}s.append(cls.model_validate(doc))
                    except PydanticValidationError as e:
                        # Convert Pydantic errors to notifications
                        for error in e.errors():
                            notify_validation_error(
                                message=f"Validation failed for field '{error['loc'][-1]}': {error['msg']}",
                                entity="{{Entity}}",
                                field=str(error['loc'][-1]),
                                value=error.get('input')
                            )
                            validation_errors.append(ValidationError(
                                message=f"Validation failed for field '{error['loc'][-1]}': {error['msg']}",
                                entity="{{Entity}}",
                                invalid_fields=[ValidationFailure(
                                    field=str(error['loc'][-1]),
                                    message=error['msg'],
                                    value=error.get('input')
                                )]
                            ))
                        # Create instance without validation for failed docs
                        {{EntityLower}}s.append(cls(**doc))
            else:
                {{EntityLower}}s = [cls(**doc) for doc in raw_docs]  # NO validation
            
            # Add database warnings to validation errors
            validation_errors.extend([ValidationError(message=w, entity="{{Entity}}", invalid_fields=[]) for w in warnings])
            return {{EntityLower}}s, validation_errors
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
                    for error in e.errors():
                        notify_validation_error(
                            message=f"Validation failed for field '{error['loc'][-1]}': {error['msg']}",
                            entity="{{Entity}}",
                            field=str(error['loc'][-1]),
                            value=error.get('input'),
                            operation="get"
                        )
                    return cls(**raw_doc), warnings  # Fallback to no validation
            else:
                return cls(**raw_doc), warnings  # NO validation
        except NotFoundError:
            raise
        except Exception as e:
            raise DatabaseError(str(e), "{{Entity}}", "get")

    async def save(self) -> tuple[Self, List[str]]:
        try:
            _, unique_validations = Config.validations(True)
            unique_constraints = self._metadata.get('uniques', []) if unique_validations else []
            
            {{AutoUpdateLines}}
            
            # VALIDATE the instance BEFORE saving to prevent bad data in DB
            try:
                # This validates all fields and raises PydanticValidationError if invalid
                validated_instance = self.__class__.model_validate(self.model_dump())
                # Use the validated data for save
                data = validated_instance.model_dump()
            except PydanticValidationError as e:
                # Convert to notifications and ValidationError format
                for err in e.errors():
                    notify_validation_error(
                        message=f"Validation failed for field '{err['loc'][-1]}': {err['msg']}",
                        entity="{{Entity}}",
                        field=str(err["loc"][-1]),
                        value=err.get("input"),
                        operation="save"
                    )
                failures = [ValidationFailure(field=str(err["loc"][-1]), message=err["msg"], value=err.get("input")) for err in e.errors()]
                raise ValidationError(message="Validation failed before save", entity="{{Entity}}", invalid_fields=failures)
            
            # Save document with unique constraints - pass complete data
            result, warnings = await DatabaseFactory.save_document("{{EntityLower}}", data, unique_constraints)

            # Update ID from result
            if not self.id and result and isinstance(result, dict) and result.get(DatabaseFactory.get_id_field()):
                self.id = result[DatabaseFactory.get_id_field()]

            return self, warnings
        except ValidationError:
            # Re-raise validation errors directly
            raise
        except Exception as e:
            raise DatabaseError(str(e), "{{Entity}}", "save")
 
    @classmethod
    async def delete(cls, {{EntityLower}}_id: str) -> tuple[bool, List[str]]:
        if not {{EntityLower}}_id:
            raise ValidationError(
                message="Cannot delete {{EntityLower}} without ID",
                entity="{{Entity}}",
                invalid_fields=[ValidationFailure("id", "ID is required for deletion", None)]
            )
        try:
            result = await DatabaseFactory.delete_document("{{EntityLower}}", {{EntityLower}}_id)
            if not result:
                raise NotFoundError("{{Entity}}", {{EntityLower}}_id)
            return True, []
        except NotFoundError:
            raise
        except Exception as e:
            raise DatabaseError(str(e), "{{Entity}}", "delete")
