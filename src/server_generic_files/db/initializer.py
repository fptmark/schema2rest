import logging
from typing import Dict, List, Set
import importlib
import pkgutil
from pathlib import Path

from .base import DatabaseInterface
from ..errors import DatabaseError


class DatabaseInitializer:
    """
    Database initialization utility that manages collections/indexes
    based on model metadata.
    """
    
    def __init__(self, db: DatabaseInterface):
        self.db = db
        self.logger = logging.getLogger(__name__)
    
    async def initialize_database(self) -> None:
        """
        Initialize database by:
        1. Discovering required collections and their indexes from model metadata
        2. Getting existing collections from database
        3. Creating missing collections
        4. Removing unused collections
        5. Managing indexes within each collection
        """
        self.logger.info("Starting database initialization...")
        
        try:
            # Discover required collections and indexes from models
            required_structure = await self._discover_required_structure()
            required_collections = set(required_structure.keys())
            self.logger.info(f"Required collections: {sorted(required_collections)}")
            
            # Get existing collections from database
            existing_collections = set(await self.db.list_collections())
            self.logger.info(f"Existing collections: {sorted(existing_collections)}")
            
            # Calculate differences
            missing_collections = required_collections - existing_collections
            unused_collections = existing_collections - required_collections
            
            # Create missing collections
            if missing_collections:
                self.logger.info(f"Creating missing collections: {sorted(missing_collections)}")
                await self._create_collections(missing_collections)
            else:
                self.logger.info("No missing collections to create")
            
            # Remove unused collections
            if unused_collections:
                self.logger.info(f"Removing unused collections: {sorted(unused_collections)}")
                await self._remove_collections(unused_collections)
            else:
                self.logger.info("No unused collections to remove")
            
            # Manage indexes for each required collection
            self.logger.info("Managing indexes for collections...")
            for collection, required_indexes in required_structure.items():
                await self._manage_collection_indexes(collection, required_indexes)
            
            self.logger.info("Database initialization completed successfully")
            
        except Exception as e:
            self.logger.error(f"Database initialization failed: {str(e)}")
            raise DatabaseError(
                message=f"Failed to initialize database: {str(e)}",
                entity="database",
                operation="initialize"
            )
    
    async def _discover_required_structure(self) -> Dict[str, List[Dict[str, any]]]:
        """
        Discover required collections and their indexes by examining model metadata.
        
        Returns:
            Dict mapping collection names to their required indexes
        """
        collections_structure = {}
        
        # Import models module to discover all models
        try:
            models_module = importlib.import_module('app.models')
            models_path = Path(models_module.__file__).parent
            
            # Iterate through all Python files in models directory
            for finder, name, ispkg in pkgutil.iter_modules([str(models_path)]):
                if name.endswith('_model'):
                    try:
                        # Import the model module
                        module = importlib.import_module(f'app.models.{name}')
                        
                        # Look for classes with _metadata attribute
                        for attr_name in dir(module):
                            attr = getattr(module, attr_name)
                            if (hasattr(attr, '_metadata') and 
                                isinstance(attr._metadata, dict) and
                                'entity' in attr._metadata):
                                
                                # Get collection name from Settings.name or derive from entity
                                collection_name = None
                                if hasattr(attr, 'Settings') and hasattr(attr.Settings, 'name'):
                                    collection_name = attr.Settings.name
                                else:
                                    # Derive collection name from entity name (lowercase)
                                    entity_name = attr._metadata['entity']
                                    collection_name = entity_name.lower()
                                
                                if collection_name:
                                    # Extract unique constraints from metadata
                                    unique_constraints = attr._metadata.get('uniques', [])
                                    required_indexes = []
                                    
                                    # Add unique constraint indexes
                                    for constraint_fields in unique_constraints:
                                        if isinstance(constraint_fields, list) and constraint_fields:
                                            index_name = f"unique_{'_'.join(constraint_fields)}"
                                            required_indexes.append({
                                                'name': index_name,
                                                'fields': constraint_fields,
                                                'unique': True
                                            })
                                    
                                    collections_structure[collection_name] = required_indexes
                                    self.logger.debug(f"Found model {attr_name} -> collection '{collection_name}' with {len(required_indexes)} unique indexes")
                    
                    except Exception as e:
                        self.logger.warning(f"Failed to import model {name}: {str(e)}")
                        continue
        
        except Exception as e:
            self.logger.error(f"Failed to discover models: {str(e)}")
            raise
        
        return collections_structure
    
    async def _create_collections(self, collections: Set[str]) -> None:
        """
        Create missing collections.
        
        Args:
            collections: Set of collection names to create
        """
        for collection in sorted(collections):
            try:
                created = await self.db.create_collection(collection)
                if created:
                    self.logger.info(f"✓ Created collection: {collection}")
                else:
                    self.logger.info(f"⚠ Collection already exists: {collection}")
            except Exception as e:
                self.logger.error(f"✗ Failed to create collection '{collection}': {str(e)}")
                raise
    
    async def _remove_collections(self, collections: Set[str]) -> None:
        """
        Remove unused collections.
        
        Args:
            collections: Set of collection names to remove
        """
        for collection in sorted(collections):
            try:
                # Skip system collections (those starting with .)
                if collection.startswith('.'):
                    self.logger.info(f"⚠ Skipping system collection: {collection}")
                    continue
                
                deleted = await self.db.delete_collection(collection)
                if deleted:
                    self.logger.info(f"✓ Deleted collection: {collection}")
                else:
                    self.logger.info(f"⚠ Collection didn't exist: {collection}")
            except Exception as e:
                self.logger.error(f"✗ Failed to delete collection '{collection}': {str(e)}")
                raise

    async def _manage_collection_indexes(self, collection: str, required_indexes: List[Dict[str, any]]) -> None:
        """
        Manage indexes for a specific collection.
        
        Args:
            collection: Collection name
            required_indexes: List of required index definitions
        """
        self.logger.info(f"\n--- Managing indexes for collection '{collection}' ---")
        
        if not required_indexes:
            self.logger.info(f"  No indexes required for '{collection}'")
            return
        
        # List required indexes
        self.logger.info(f"  Required indexes for '{collection}':")
        for idx in required_indexes:
            fields_str = " + ".join(idx['fields']) if len(idx['fields']) > 1 else idx['fields'][0]
            unique_str = " (UNIQUE)" if idx['unique'] else ""
            self.logger.info(f"    - {idx['name']}: {fields_str}{unique_str}")
        
        try:
            # Check if collection exists
            if not await self.db.collection_exists(collection):
                self.logger.warning(f"  Collection '{collection}' doesn't exist, skipping index management")
                return
            
            # Get existing indexes
            try:
                existing_indexes = await self.db.list_indexes(collection)
                self.logger.info(f"  Existing indexes for '{collection}':")
                if existing_indexes:
                    for idx in existing_indexes:
                        fields_str = " + ".join(idx['fields']) if len(idx['fields']) > 1 else idx['fields'][0]
                        unique_str = " (UNIQUE)" if idx.get('unique') else ""
                        system_str = " (SYSTEM)" if idx.get('system') else ""
                        self.logger.info(f"    - {idx['name']}: {fields_str}{unique_str}{system_str}")
                else:
                    self.logger.info(f"    - No existing indexes found")
            except Exception as e:
                self.logger.warning(f"  Could not list existing indexes for '{collection}': {str(e)}")
                existing_indexes = []
            
            # Create missing indexes
            existing_index_names = {idx['name'] for idx in existing_indexes}
            for required_idx in required_indexes:
                if required_idx['name'] not in existing_index_names:
                    try:
                        created = await self.db.create_index(
                            collection, 
                            required_idx['fields'], 
                            unique=required_idx['unique']
                        )
                        if created:
                            fields_str = " + ".join(required_idx['fields'])
                            self.logger.info(f"  ✓ Created index '{required_idx['name']}' on {fields_str}")
                        else:
                            self.logger.info(f"  ⚠ Index '{required_idx['name']}' already exists or couldn't be created")
                    except Exception as e:
                        self.logger.error(f"  ✗ Failed to create index '{required_idx['name']}': {str(e)}")
                else:
                    self.logger.info(f"  ✓ Index '{required_idx['name']}' already exists")
            
            # Note: We don't delete unused indexes automatically as they might be manually created
            # and serve other purposes beyond what's defined in model metadata
            
        except Exception as e:
            self.logger.error(f"Failed to manage indexes for collection '{collection}': {str(e)}")
            raise