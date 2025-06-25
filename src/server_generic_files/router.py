"""
Simplified dynamic router factory that reads entity names from schema.yaml.

This module creates FastAPI routers dynamically based on entity names from schema.yaml,
eliminating the need for metadata services, reflection, or async complexity.
"""

import yaml
from pathlib import Path
from fastapi import APIRouter
from typing import Dict, Any, Type, List
import importlib
import logging
from app.notification import NotificationManager, notify_success, notify_error, notify_warning, NotificationType
from app.errors import ValidationError, NotFoundError, DuplicateError, DatabaseError

logger = logging.getLogger(__name__)


def get_entity_names(schema_path: Path) -> List[str]:
    """
    Get entity names directly from schema.yaml.
    
    Returns:
        List of entity names (e.g., ['User', 'Account', 'Event'])
    """
    
    try:
        with open(schema_path, 'r') as f:
            schema = yaml.safe_load(f)
        
        entity_names = [ 
            name for name, attrs in schema.get('_entities', {}).items()
            if not attrs.get('abstract', False) 
        ]
        logger.info(f"Found {len(entity_names)} entities in schema.yaml: {entity_names}")
        return entity_names
        
    except FileNotFoundError:
        logger.error(f"schema.yaml not found at {schema_path}")
        return []
    except yaml.YAMLError as e:
        logger.error(f"Invalid YAML in schema.yaml: {e}")
        return []
    except Exception as e:
        logger.error(f"Failed to read schema.yaml: {e}")
        return []


class SimpleDynamicRouterFactory:
    """Factory for creating entity-specific routers from schema.yaml entity names."""
    
    @staticmethod
    def _import_model_class(entity_name: str) -> Type:
        """Dynamically import the main model class."""
        try:
            module_name = f"app.models.{entity_name.lower()}_model"
            module = importlib.import_module(module_name)
            return getattr(module, entity_name)
        except (ImportError, AttributeError) as e:
            logger.error(f"Failed to import model class {entity_name}: {e}")
            raise ImportError(f"Could not import model class {entity_name}")
    
    @staticmethod
    def _import_create_class(entity_name: str) -> Type:
        """Dynamically import the Create model class."""
        try:
            module_name = f"app.models.{entity_name.lower()}_model"
            module = importlib.import_module(module_name)
            return getattr(module, f"{entity_name}Create")
        except (ImportError, AttributeError) as e:
            logger.error(f"Failed to import create class {entity_name}Create: {e}")
            raise ImportError(f"Could not import create class {entity_name}Create")
    
    @staticmethod
    def _import_update_class(entity_name: str) -> Type:
        """Dynamically import the Update model class."""
        try:
            module_name = f"app.models.{entity_name.lower()}_model"
            module = importlib.import_module(module_name)
            return getattr(module, f"{entity_name}Update")
        except (ImportError, AttributeError) as e:
            logger.error(f"Failed to import update class {entity_name}Update: {e}")
            raise ImportError(f"Could not import update class {entity_name}Update")
    
    @classmethod
    def create_entity_router(cls, entity_name: str) -> APIRouter:
        """
        Create a complete CRUD router for an entity.
        
        Args:
            entity_name: The entity name (e.g., "User", "Account")
            
        Returns:
            FastAPI router with all CRUD endpoints for the entity
        """
        router = APIRouter(
            prefix=f"/{entity_name.lower()}", 
            tags=[entity_name]
        )
        
        # Dynamic model imports
        try:
            entity_cls = cls._import_model_class(entity_name)
            create_cls = cls._import_create_class(entity_name)
            update_cls = cls._import_update_class(entity_name)
        except ImportError as e:
            logger.error(f"Failed to import classes for {entity_name}: {e}")
            # Return empty router if imports fail
            return router
        
        entity_lower = entity_name.lower()
        
        # LIST endpoint
        async def list_entities() -> Dict[str, Any]:
            """List all entities of this type."""
            notifications = NotificationManager().start_operation(f"list_{entity_lower}s", entity_name)
            
            try:
                entities, validation_errors = await entity_cls.get_all()
                
                # Add any validation errors as warnings
                for error in validation_errors:
                    notify_warning(str(error), NotificationType.VALIDATION)
                
                return notifications.to_response([entity.model_dump() for entity in entities])
            except Exception as e:
                notify_error(f"Failed to retrieve {entity_lower}s: {str(e)}", NotificationType.SYSTEM)
                return notifications.to_response(None)
            finally:
                NotificationManager().end_operation()
        
        # GET endpoint
        async def get_entity(entity_id: str) -> Dict[str, Any]:
            """Get a specific entity by ID."""
            notifications = NotificationManager().start_operation(f"get_{entity_lower}", entity_name)
            
            try:
                entity, warnings = await entity_cls.get(entity_id)
                
                # Add any warnings as notifications
                for warning in warnings:
                    notify_warning(warning, NotificationType.DATABASE)
                
                return notifications.to_response(entity.model_dump())
            except NotFoundError:
                notify_error(f"{entity_name} not found", NotificationType.BUSINESS)
                return notifications.to_response(None)
            except Exception as e:
                notify_error(f"Failed to retrieve {entity_lower}: {str(e)}", NotificationType.SYSTEM)
                return notifications.to_response(None)
            finally:
                NotificationManager().end_operation()
        
        # POST endpoint
        async def create_entity(entity_data) -> Dict[str, Any]:  # Type will be set dynamically
            """Create a new entity."""
            notifications = NotificationManager().start_operation(f"create_{entity_lower}", entity_name)
            
            try:
                # Let model handle all validation and business logic
                entity = entity_cls(**entity_data.model_dump())
                result = await entity.save()
                notify_success(f"{entity_name} created successfully", NotificationType.BUSINESS)
                return notifications.to_response(result.model_dump())
            except (ValidationError, DuplicateError) as e:
                notify_error(f"Failed to create {entity_lower}: {str(e)}", NotificationType.VALIDATION)
                return notifications.to_response(None)
            except Exception as e:
                notify_error(f"Failed to create {entity_lower}: {str(e)}", NotificationType.SYSTEM)
                return notifications.to_response(None)
            finally:
                NotificationManager().end_operation()
        
        # PUT endpoint
        async def update_entity(entity_id: str, entity_data) -> Dict[str, Any]:  # Type will be set dynamically
            """Update an existing entity."""
            notifications = NotificationManager().start_operation(f"update_{entity_lower}", entity_name)
            
            try:
                existing, warnings = await entity_cls.get(entity_id)
                # Add any warnings from get operation
                for warning in warnings:
                    notify_warning(warning, NotificationType.DATABASE)
                    
                # Merge payload data into existing entity - model handles all logic
                updated = existing.model_copy(update=entity_data.model_dump())
                result = await updated.save()
                notify_success(f"{entity_name} updated successfully", NotificationType.BUSINESS)
                return notifications.to_response(result.model_dump())
            except (NotFoundError, ValidationError, DuplicateError) as e:
                notify_error(f"Failed to update {entity_lower}: {str(e)}", NotificationType.VALIDATION)
                return notifications.to_response(None)
            except Exception as e:
                notify_error(f"Failed to update {entity_lower}: {str(e)}", NotificationType.SYSTEM)
                return notifications.to_response(None)
            finally:
                NotificationManager().end_operation()
        
        # DELETE endpoint
        async def delete_entity(entity_id: str) -> Dict[str, Any]:
            """Delete an entity."""
            notifications = NotificationManager().start_operation(f"delete_{entity_lower}", entity_name)
            
            try:
                success, warnings = await entity_cls.delete(entity_id)
                if success:
                    notify_success(f"{entity_name} deleted successfully", NotificationType.BUSINESS)
                for warning in warnings:
                    notify_warning(warning, NotificationType.DATABASE)
                return notifications.to_response(None)
            except NotFoundError:
                notify_error(f"{entity_name} not found", NotificationType.BUSINESS)
                return notifications.to_response(None)
            except Exception as e:
                notify_error(f"Failed to delete {entity_lower}: {str(e)}", NotificationType.SYSTEM)
                return notifications.to_response(None)
            finally:
                NotificationManager().end_operation()
        
        # Register routes with proper typing for OpenAPI
        router.add_api_route(
            "",
            list_entities,
            methods=["GET"],
            summary=f"List all {entity_lower}s",
            response_description=f"List of {entity_lower}s"
        )
        
        router.add_api_route(
            "/{entity_id}",
            get_entity,
            methods=["GET"],
            summary=f"Get a specific {entity_lower} by ID",
            response_description=f"The requested {entity_lower}"
        )
        
        # Set proper type annotations for OpenAPI
        create_entity.__annotations__['entity_data'] = create_cls
        create_entity.__annotations__['return'] = Dict[str, Any]
        
        update_entity.__annotations__['entity_data'] = update_cls
        update_entity.__annotations__['return'] = Dict[str, Any]
        
        router.add_api_route(
            "",
            create_entity,
            methods=["POST"],
            summary=f"Create a new {entity_lower}",
            response_description=f"The created {entity_lower}"
        )
        
        router.add_api_route(
            "/{entity_id}",
            update_entity,
            methods=["PUT"],
            summary=f"Update an existing {entity_lower}",
            response_description=f"The updated {entity_lower}"
        )
        
        router.add_api_route(
            "/{entity_id}",
            delete_entity,
            methods=["DELETE"],
            summary=f"Delete a {entity_lower}",
            response_description="Deletion confirmation"
        )
        
        logger.info(f"Created dynamic router for entity: {entity_name}")
        return router
    
    @classmethod
    def create_all_routers(cls, schema_path: Path) -> List[APIRouter]:
        """
        Create routers for all entities found in schema.yaml.
        
        Returns:
            List of FastAPI routers, one for each entity
        """
        entity_names = get_entity_names(schema_path)
        routers = []
        
        for entity_name in entity_names:
            try:
                router = cls.create_entity_router(entity_name)
                routers.append(router)
                logger.info(f"Successfully created router for: {entity_name}")
            except Exception as e:
                logger.warning(f"Skipping {entity_name} - failed to create router: {e}")
                continue
        
        logger.info(f"Created {len(routers)} dynamic routers from {len(entity_names)} entities")
        return routers


# Convenience function for easy integration
def get_all_dynamic_routers(schema_path: Path) -> List[APIRouter]:
    """
    Get all dynamic routers for entities in schema.yaml.
    
    This is the main entry point for getting dynamic routers.
    Call this at module level in main.py.
    
    Returns:
        List of FastAPI routers ready to be registered
    """
    return SimpleDynamicRouterFactory.create_all_routers(schema_path)