import sys
import config
import pymongo
from pymongo.errors import DuplicateKeyError
import schema

def run(schema):

    conf = config.load_config() 
    db_name = conf.get("db_name", "default_db")
    mongo_uri = conf.get("mongo_uri", "mongodb://localhost:27017")
    client = pymongo.MongoClient(mongo_uri)
    db = client[db_name]

    # 2) Iterate over each entity (collection) in the schema
    for entity, data in schema.concrete_entities().items():

        collection = db[entity]
        print(f"Processing collection: {entity}")

        # Gather definitions of the unique indexes needed from the schema
        uniques = data.get("uniques", [])
        # Each 'unique_def' has a 'fields' list, e.g. ["field1", "field2"]

        # Build a set of needed index definitions: {((field1, ASC), (field2, ASC)), is_unique_boolean}
        # We'll always use ASCENDING for each field here.
        needed_indexes = []
        for fields in uniques:
            if fields:
                index_tuple = tuple((field, pymongo.ASCENDING) for field in fields)
                needed_indexes.append((index_tuple, True))  # True => unique index

        # Convert to a set for quick membership checks
        needed_indexes_set = set(needed_indexes)

        # 3) Get existing indexes from the collection
        existing_indexes = collection.index_information()
        # Example structure of index_information():
        # {
        #   '_id_': {'key': [('_id', 1)], 'v': 2},
        #   'email_1': {'key': [('email', 1)], 'unique': True, 'v': 2},
        #   ...
        # }

        # Parse existing indexes into a dict of:
        #   index_name -> ( (field1, ASC), (field2, ASC) ..., is_unique_bool )
        parsed_existing = {}
        for idx_name, idx_info in existing_indexes.items():
            key_fields = tuple(idx_info['key'])  # e.g. (("email", 1),)
            is_unique = idx_info.get('unique', False)
            parsed_existing[idx_name] = (key_fields, is_unique)

        # 4) Drop only those indexes that we truly do NOT need
        for idx_name, (key_fields, is_unique) in parsed_existing.items():
            # Always keep _id_
            if idx_name == "_id_":
                continue

            # If the existing index definition isn't in our needed set, drop it
            if (key_fields, is_unique) not in needed_indexes_set:
                print(f"  Dropping unused index '{idx_name}' => fields: {key_fields}, unique={is_unique}")
                collection.drop_index(idx_name)

        # 5) Create only the new needed unique indexes that don't already exist
        #    We'll build a reversed map from index definitions to index names
        #    so we can quickly see if an index is already present.
        existing_defs_to_name = {v: k for k, v in parsed_existing.items()}

        for needed_def in needed_indexes:
            if needed_def not in existing_defs_to_name:
                # This needed index does not exist yet, so create it
                (fields_tuple, is_unique) = needed_def
                print(f"  Creating index for {fields_tuple}, unique={is_unique}")

                try:
                    collection.create_index(fields_tuple, unique=is_unique)
                except DuplicateKeyError as e:
                    print(f"ERROR: Could not create unique index on '{entity}' for {fields_tuple}")
                    print(f"DuplicateKeyError: {e}")
                    print("Exiting due to duplicate key conflict.")
                    sys.exit(1)

    print("Index update process completed successfully.")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python update_indexes.py <schema_path>")
        sys.exit(1)
    my_schema = schema.Schema(sys.argv[1])
    run(my_schema)