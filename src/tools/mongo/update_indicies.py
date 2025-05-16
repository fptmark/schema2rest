import sys
from pathlib import Path
from utilities.utils import load_system_config
import pymongo
from pymongo.errors import DuplicateKeyError
import json

# Add parent directory to path to allow importing helpers
# sys.path.append(str(Path(__file__).parent))
from common import Schema

def update_indexes(schema_file: str, config_file: str):
    schema = Schema(schema_file)
    config = load_system_config(config_file)

    client = pymongo.MongoClient(config.get("db_uri", "mongodb://localhost:27017"))
    db = client[config.get("db_name", "default_db")]

    for entity, data in schema.concrete_entities().items():
        entity = entity.lower()
        collection = db[entity]
        print(f"Processing collection: {entity}")

        # Gather definitions of the unique indexes needed from the schema
        uniques = data.get("unique", [])
        needed_indexes = []
        for fields in uniques:
            if fields:
                index_tuple = tuple((field, pymongo.ASCENDING) for field in fields)
                needed_indexes.append((index_tuple, True))  # True => unique index

        # Convert to a set for quick membership checks
        needed_indexes_set = set(needed_indexes)

        # Get existing indexes from the collection
        existing_indexes = collection.index_information()

        # Parse existing indexes
        parsed_existing = {}
        for idx_name, idx_info in existing_indexes.items():
            key_fields = tuple(idx_info['key'])
            is_unique = idx_info.get('unique', False)
            parsed_existing[idx_name] = (key_fields, is_unique)

        # Drop indexes that are not needed
        for idx_name, (key_fields, is_unique) in parsed_existing.items():
            # Always keep _id_
            if idx_name == "_id_":
                continue

            # If the existing index definition isn't in our needed set, drop it
            if (key_fields, is_unique) not in needed_indexes_set:
                print(f"  Dropping unused index '{idx_name}' => fields: {key_fields}, unique={is_unique}")
                collection.drop_index(idx_name)

        # Create new needed unique indexes
        existing_defs_to_name = {v: k for k, v in parsed_existing.items()}

        for needed_def in needed_indexes:
            (fields_tuple, is_unique) = needed_def
            
            # Check if index already exists
            if needed_def not in existing_defs_to_name:
                print(f"  Creating index for {fields_tuple}, unique={is_unique}")
                
                try:
                    # Create the index
                    index_name = collection.create_index(
                        list(fields_tuple), 
                        unique=is_unique
                    )
                    
                    # Verify index creation
                    indexes = list(collection.list_indexes())
                    matching_indexes = [
                        idx for idx in indexes 
                        if set(tuple(k) if isinstance(k, tuple) else (k, 1) for k in idx['key']) == set(fields_tuple)
                    ]
                    
                    if matching_indexes:
                        print("  ✓ Index verified successfully")
                        # print("    Index details:", json.dumps(matching_indexes[0], indent=2))
                    else:
                        print("  ✗ Failed to verify index creation")
                
                except pymongo.errors.OperationFailure as e:
                    print(f"  ERROR creating index: {e}")
                    print(f"  Error details: {type(e).__name__}")
                    print(f"  Error args: {e.args}")
                except Exception as e:
                    print(f"  UNEXPECTED ERROR: {e}")
                    print(f"  Error type: {type(e).__name__}")
                    print(f"  Error args: {e.args}")

    print("Index update process completed successfully.")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python update_indexes.py <schema_path>")
        sys.exit(1)
    update_indexes(sys.argv[1], 'config.json' if len(sys.argv) < 3 else sys.argv[2])