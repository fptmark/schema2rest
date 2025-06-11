from fastapi import APIRouter
from typing import List
import logging
from app.models.{{entity_lower}}_model import {{entity}}, {{entity}}Create, {{entity}}Update
from app.errors import ValidationError, NotFoundError, DuplicateError, DatabaseError

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("", response_model=List[{{entity}}])
async def list_{{entity_lower}}s() -> List[{{entity}}]:
    """List all {{entity_lower}}s"""
    try:
        logger.info("Fetching all {{entity_lower}}s")
        {{entity_lower}}s = await {{entity}}.find_all()
        records = len({{entity_lower}}s)
        logger.info(f"Retrieved {records} {{entity_lower}}s")
        return list({{entity_lower}}s)
    except Exception as e:
        logger.error(f"Error listing {{entity_lower}}s: {e}")
        raise


@router.get("/{ {{entity_lower}}_id }", response_model={{entity}})
async def get_{{entity_lower}}({{entity_lower}}_id: str) -> {{entity}}:
    """Get a specific {{entity_lower}} by ID"""
    try:
        logger.info(f"Fetching {{entity_lower}} with ID: { {{entity_lower}}_id }")
        {{entity_lower}} = await {{entity}}.get({{entity_lower}}_id)
        logger.info(f"Retrieved {{entity_lower}}: { {{entity_lower}}.id }")
        return {{entity_lower}}
    except NotFoundError:
        logger.warning(f"{{entity}} not found: { {{entity_lower}}_id }")
        raise
    except Exception as e:
        logger.error(f"Error getting {{entity_lower}} { {{entity_lower}}_id }: {e}")
        raise


@router.post("", response_model={{entity}})
async def create_{{entity_lower}}({{entity_lower}}_data: {{entity}}Create) -> {{entity}}:
    """Create a new {{entity_lower}}"""
    try:
        logger.info(f"Creating {{entity_lower}} with data: { {{entity_lower}}_data }")
        {{entity_lower}} = {{entity}}(**{{entity_lower}}_data.model_dump())
        result = await {{entity_lower}}.save()
        logger.info(f"{{entity}} created successfully with ID: {result.id}")
        return result
    except (ValidationError, DuplicateError) as e:
        logger.warning(f"Validation error creating {{entity_lower}}: {type(e).__name__}: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error creating {{entity_lower}}: {e}")
        raise


@router.put("/{ {{entity_lower}}_id }", response_model={{entity}})
async def update_{{entity_lower}}({{entity_lower}}_id: str, {{entity_lower}}_data: {{entity}}Update) -> {{entity}}:
    """Update an existing {{entity_lower}}"""
    try:
        logger.info(f"Updating {{entity_lower}} { {{entity_lower}}_id } with data: { {{entity_lower}}_data }")

        existing = await {{entity}}.get({{entity_lower}}_id)
        logger.info(f"Found existing {{entity_lower}}: {existing.id}")

        {{entity_lower}} = {{entity}}(**{{entity_lower}}_data.model_dump())
        result = await {{entity_lower}}.save({{entity_lower}}_id)
        logger.info(f"{{entity}} updated successfully: {result.id}")
        return result
    except (NotFoundError, ValidationError, DuplicateError) as e:
        logger.warning(f"Error updating {{entity_lower}} { {{entity_lower}}_id }: {type(e).__name__}: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error updating {{entity_lower}} { {{entity_lower}}_id }: {e}")
        raise


@router.delete("/{ {{entity_lower}}_id }")
async def delete_{{entity_lower}}({{entity_lower}}_id: str):
    """Delete a {{entity_lower}}"""
    try:
        logger.info(f"Deleting {{entity_lower}}: { {{entity_lower}}_id }")
        {{entity_lower}} = await {{entity}}.get({{entity_lower}}_id)
        await {{entity_lower}}.delete()
        logger.info(f"{{entity}} deleted successfully: { {{entity_lower}}_id }")
        return {"message": "{{entity}} deleted successfully"}
    except NotFoundError:
        logger.warning(f"{{entity}} not found for deletion: { {{entity_lower}}_id }")
        raise