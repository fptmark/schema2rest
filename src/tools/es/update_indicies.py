import sys
from pathlib import Path
from utilities.utils import load_system_config
from elasticsearch import Elasticsearch
from common import Schema

def update_indexes(schema_file, config_file):
    """
    Update Elasticsearch indexes following the same rules as MongoDB:
    1. Identify needed indexes from schema
    2. Remove indexes that are no longer needed
    3. Create only the indexes that don't already exist
    """
    schema = Schema(schema_file)
    config = load_system_config(config_file)

    client = Elasticsearch(config.get("es_uri", "http://localhost:9200"))
    
    for entity, data in schema.concrete_entities().items():
        # Gather definitions of the unique indexes needed from the schema
        uniques = data.get("unique", [])
        needed_indexes = []
        for fields in uniques:
            if fields:
                index_tuple = tuple(fields)
                needed_indexes.append((index_tuple, True))  # True => unique index

        # Convert to a set for quick membership checks
        needed_indexes_set = set(needed_indexes)

        # Get existing indexes from the index
        existing_indexes = client.indices.get_mapping(index=entity)
        
        # Parse existing indexes
        parsed_existing = {}
        for idx_name, idx_info in existing_indexes.items():
            # Extract index fields and unique status
            # This will depend on Elasticsearch's specific mapping structure
            key_fields = tuple(idx_info.get('mappings', {}).keys())
            is_unique = False  # Elasticsearch handles uniqueness differently
            parsed_existing[idx_name] = (key_fields, is_unique)

        # Drop indexes that are not needed
        for idx_name, (key_fields, is_unique) in parsed_existing.items():
            # Always keep the default index
            if idx_name == entity:
                continue

            # If the existing index definition isn't in our needed set, drop it
            if (key_fields, is_unique) not in needed_indexes_set:
                print(f"  Dropping unused index '{idx_name}' => fields: {key_fields}")
                # Elasticsearch index deletion method
                client.indices.delete(index=idx_name)

        # Create new needed indexes that don't exist
        existing_defs_to_name = {v: k for k, v in parsed_existing.items()}

        for needed_def in needed_indexes:
            (fields_tuple, is_unique) = needed_def
            
            # Check if index already exists
            if needed_def not in existing_defs_to_name:
                print(f"  Creating index for {fields_tuple}, unique={is_unique}")
                
                try:
                    # Create the index
                    # Elasticsearch index creation will depend on specific requirements
                    index_name = f"{entity}_{'_'.join(fields_tuple)}"
                    client.indices.create(
                        index=index_name,
                        body={
                            "mappings": {
                                "properties": {
                                    field: {"type": "keyword"} for field in fields_tuple
                                }
                            }
                        }
                    )
                    
                    # Verify index creation
                    # This will depend on Elasticsearch's method of checking index existence
                    if client.indices.exists(index=index_name):
                        print("  ✓ Index verified successfully")
                    else:
                        print("  ✗ Failed to verify index creation")
                
                except Exception as e:
                    print(f"  ERROR creating index: {e}")
                    print(f"  Error type: {type(e).__name__}")
                    print(f"  Error args: {e.args}")

    print("Index update process completed successfully.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python update_indexes.py <schema_path>")
        sys.exit(1)
    update_indexes(sys.argv[1], 'config.json' if len(sys.argv) < 3 else sys.argv[2])
