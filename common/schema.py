import yaml

class Schema:
    
    RESERVED_TYPES = { "ISODate", "ObjectId" }
    
    def __init__(self, schema_path: str):
        self.schema = {}
        self.metadata: dict[str, dict] = {}
        with open(schema_path, "r") as file:
            self.schema = yaml.safe_load(file)
            # self.extract_metadata() # move metadata from schema and return just the metadata
    
    # def extract_metadata(self):
    #     self.metadata = {}
    #     for entity_name, entity in self.schema.get("_entities", {}).items():
    #         ui_fields = {}
    #         fields = entity.get('fields', {})
    #         for field_name, field_def in fields.items():
    #             # If ui_metadata exists, pop it (remove from original) and add it to the ui-only dict.
    #             if 'ui_metadata' in field_def:
    #                 ui_fields[field_name] = {'ui_metadata': field_def.pop('ui_metadata')}
    #         # Store the ui_metadata for the entity (if any)
    #         self.metadata[entity_name] = {'fields': ui_fields}

    # def _get_metadata(self, entity_name: str, field_name: str) -> dict:
    #     return self.metadata.get(entity_name, {}).get('fields', {}).get(field_name, {}) if self.metadata else {}

    def concrete_entities(self, reserved_types=RESERVED_TYPES) -> dict:
        entities = self.all_entities(reserved_types)
        # Remove all inherited entities from concrete entities
        for inherited in self.inherited_entities(reserved_types):
            if inherited in entities:
                del entities[inherited]
        return entities

    def inherited_entities(self, reserved_types=RESERVED_TYPES) -> dict:
        return self.schema['_inherited_entities']

    def all_entities(self, reserved_types=RESERVED_TYPES) -> dict:
        return self._get_attribute('_entities', reserved_types)

    def entity(self, entity_name: str, reserved_types=RESERVED_TYPES) -> dict:
        return self.all_entities(reserved_types)[entity_name]

    def _get_attribute(self, entity_type: str, reserved_types=RESERVED_TYPES) -> dict:
        entity_obj = self._get_object(entity_type)
        
        # Extract entity schemas, skipping reserved types and metadata keys
        entity_schemas = {
            name: details for name, details in entity_obj.items()
            if name not in reserved_types and isinstance(details, dict)
        }
        
        return entity_schemas

    def relationships(self):
        return self._get_object('_relationships')

    def dictionaries(self):
        return self._get_object('_dictionaries')

    def services(self):
        return self._get_object('_services')

    def _get_object(self, object_name: str):
        return self.schema[object_name] 

    def full_schema(self):
        return self.schema

