from pathlib import Path
import sys
import yaml
import os
from typing import Dict, Set, Any, List, Tuple

# Add parent directory to path to allow importing helpers
sys.path.append(str(Path(__file__).parent.parent))
from common import helpers

# Import decorator handling
from .decorators import Decorator #, DICTIONARY, UNIQUE, VALIDATE, VALIDATOR, INHERIT, SERVICE, UI

### Define a custom formatter for quoting strings in the YAML output 
class QuotedStr(str):
    """String that will be quoted in YAML output"""
    pass

def quoted_str_representer(dumper, data):
    """Custom YAML representer for quoted strings"""
    return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='"')

yaml.add_representer(QuotedStr, quoted_str_representer)

class SchemaParser:
    """Parser for MMD schema files"""
    
    def __init__(self):
        self.entities = {}
        self.relationships = []
        self.dictionaries = {}
        self.current_entity = None
        self.current_field = None
        self.decorator = Decorator()  # Create the decorator processor
    
    def parse_mmd(self, lines):
        """
        Parse MMD schema directly
        Returns a tuple of (entities, relationships, dictionaries)
        """
        print("Starting schema parsing...")
        
        for line in lines:
            line = line.strip()
            
            # Skip empty lines
            if not line:
                continue
            
            # Entity definition
            if self._is_entity_definition(line):
                self._handle_entity_definition(line)
                continue
            
            # Entity end
            if line == "}":
                self.current_entity = None
                self.current_field = None
                continue
            
            # Skip if not in entity or just a comment (not decorator)
            if self.decorator.has_decorator(line):
                self.decorator.process_decorations(line, self.current_entity, self.current_field)
                

                continue
            if not self.current_entity or (line.startswith("%") and not line.startswith("%%")):
                # Check for dictionary definition
                if line.startswith(f"%% {DICTIONARY}"):
                    self._parse_dictionary(line)
                continue
            
            # Process field-level decorations (like @validate, @ui, etc.)
            if line.startswith("%% @"):
                self._parse_field_decorations(line)
                continue
                
            # Process field definition (without decorations)
            if not line.startswith("%%"):
                self._parse_field(line)
                continue
            
            # Process entity-level decorators
            if line.startswith(f"%% {INHERIT}"):
                self._parse_inherit_decorator(line)
            elif line.startswith(f"%% {SERVICE}"):
                self._parse_service_decorator(line)
            elif line.startswith(f"%% {UNIQUE}"):
                self._parse_unique_decorator(line)
        
        # Extract relationships from field references
        self._extract_relationships()
        
        # Apply decorations from the decorator processor
        self._apply_decorations()
        
        return self.entities, self.relationships, self.dictionaries
        
    def _apply_decorations(self):
        """Apply collected decorations to entities and fields"""
        # Get all decorations
        decorations = self.decorator.get_decorations()
        
        # Apply entity-level decorations
        for entity_name, entity_decorations in decorations.get("entity", {}).items():
            if entity_name in self.entities:
                for decoration in entity_decorations:
                    self._apply_entity_decoration(entity_name, decoration)
        
        # Apply field-level decorations
        for (entity_name, field_name), field_decorations in decorations.get("field", {}).items():
            if entity_name in self.entities and field_name in self.entities[entity_name]["fields"]:
                for decoration in field_decorations:
                    self._apply_field_decoration(entity_name, field_name, decoration)
        
        # Add dictionaries - convert values to QuotedStr
        for dict_name, dict_content in decorations.get("dictionary", {}).items():
            self.dictionaries[dict_name] = {}
            for key, value in dict_content.items():
                self.dictionaries[dict_name][key] = QuotedStr(value)
    
    def _apply_entity_decoration(self, entity_name, decoration):
        """Apply entity-level decoration to an entity"""
        # Apply specific types of decorations
        pass  # This is handled by the individual parsers currently
    
    def _apply_field_decoration(self, entity_name, field_name, decoration):
        """Apply field-level decoration to a field"""
        # Get the field data
        field_data = self.entities[entity_name]["fields"][field_name]
        
        # For each decorator type in the decoration
        for decorator_type, attrs in decoration.items():
            if decorator_type != "field_name":  # Skip our internal field name reference
                if not isinstance(attrs, dict):
                    continue
                    
                # Special handling for UI decorators
                if decorator_type == "ui":
                    # Create a ui_metadata section if it doesn't exist
                    if "ui_metadata" not in field_data:
                        field_data["ui_metadata"] = {}
                        
                    # Add all UI attributes to ui_metadata
                    for key, value in attrs.items():
                        field_data["ui_metadata"][key] = value
                else:
                    # For other decorators (validate, unique), merge attributes directly
                    for key, value in attrs.items():
                        field_data[key] = value
    
    @property
    def stats(self):
        """Get statistics from the decorator processor"""
        return self.decorator.get_stats()
    
    def _is_entity_definition(self, line):
        """Check if the line is an entity definition"""
        return line.endswith("{") and not line.startswith("%")
    
    def _handle_entity_definition(self, line):
        """Process an entity definition line"""
        self.current_entity = line.split()[0].strip()
        self.entities[self.current_entity] = {
            "fields": {},
            "relationships": []
        }
        print(f"Processing entity: {self.current_entity}")
    
    def _parse_dictionary(self, line):
        """Parse a dictionary definition"""
        # Process using decorator class
        decorations = self.decorator.process_decorations(line)
        
        # No need to do anything else here - dictionaries will be added
        # in the _apply_decorations method
        if self.decorator.has_decorator(line, DICTIONARY):
            dict_parts = line[len(f"%% {DICTIONARY}"):].strip().split(" ", 1)
            if len(dict_parts) >= 1:
                dict_name = dict_parts[0]
                print(f"Found dictionary: {dict_name}")
    
    def _parse_field(self, line):
        """Parse a field definition and check for inline decorations"""
        parts = line.split()
        if len(parts) < 2:
            return
            
        field_type = parts[0]
        field_name = parts[1]
        self.current_field = field_name
        
        # Start with field type
        field_data = {
            "type": field_type
        }
        
        # Add the field to entity
        self.entities[self.current_entity]["fields"][field_name] = field_data
        
        # Check for inline decorators (field_name %% @decorator)
        if "%%" in line:
            # Extract the decorator portion
            decorator_part = line[line.find("%%"):].strip()
            if self.decorator.is_decorator(decorator_part):
                # Process field decorations
                decorations = self.decorator.process_field_decorations(
                    self.current_entity, field_name, decorator_part)
                
                # Merge attributes from decorations into field_data
                for decorator_type, attrs in decorations.items():
                    if decorator_type != "field_name":  # Skip our internal field name reference
                        for key, value in attrs.items():
                            field_data[key] = value
                
        print(f"  Field {field_name}: Base type {field_type}")
        
    def _parse_field_decorations(self, line):
        """Parse a line with field decorations (starting with %%)"""
        if not self.decorator.is_decorator(line):
            return False
            
        # Use the most recently defined field (the current field)
        if not self.current_entity or not self.current_field or not self.current_field in self.entities[self.current_entity]["fields"]:
            print("  Warning: Field decoration without a preceding field definition")
            return True
            
        field_name = self.current_field
        field_data = self.entities[self.current_entity]["fields"][field_name]
        
        # Process decorations using the decorator class
        decorations = self.decorator.process_field_decorations(
            self.current_entity, field_name, line)
            
        # Merge attributes from decorations into field_data
        for decorator_type, attrs in decorations.items():
            if decorator_type != "field_name":  # Skip our internal field name reference
                for key, value in attrs.items():
                    field_data[key] = value
                    
        print(f"  Processed decorations for {field_name}")
        return True
        
    # These functions have been replaced by the Decorator class
    
    def _parse_inherit_decorator(self, line):
        """Parse @inherit decorator"""
        # Use the decorator class to process
        decorations = self.decorator.process_entity_decorations(self.current_entity, line)
        
        # Extract inherited entity
        if "inherit" in decorations and "inherited" in decorations["inherit"]:
            inherited = decorations["inherit"]["inherited"]
            if "inherits" not in self.entities[self.current_entity]:
                self.entities[self.current_entity]["inherits"] = []
            self.entities[self.current_entity]["inherits"].append(inherited)
            
        print(f"  Entity {self.current_entity} inherits from: {self.entities[self.current_entity].get('inherits', [])}")
    
    def _parse_service_decorator(self, line):
        """Parse @service decorator"""
        # Use the decorator class to process
        decorations = self.decorator.process_entity_decorations(self.current_entity, line)
        
        # Extract service name
        if "service" in decorations and "service" in decorations["service"]:
            service_name = decorations["service"]["service"]
            
            if "inherits" not in self.entities[self.current_entity]:
                self.entities[self.current_entity]["inherits"] = []
            
            # Check if service entry already exists
            service_entry = None
            for item in self.entities[self.current_entity]["inherits"]:
                if isinstance(item, dict) and "service" in item:
                    service_entry = item
                    break
            
            if service_entry:
                if service_name not in service_entry["service"]:
                    service_entry["service"].append(service_name)
            else:
                self.entities[self.current_entity]["inherits"].append({"service": [service_name]})
                
            print(f"  Entity {self.current_entity} uses service: {service_name}")
    
    def _parse_unique_decorator(self, line):
        """Parse @unique decorator"""
        # Use the decorator class to process
        decorations = self.decorator.process_entity_decorations(self.current_entity, line)
        
        # Extract unique fields
        if "unique" in decorations and "unique_fields" in decorations["unique"]:
            unique_fields = decorations["unique"]["unique_fields"]
            
            if "uniques" not in self.entities[self.current_entity]:
                self.entities[self.current_entity]["uniques"] = []
                
            # Add the unique fields
            if isinstance(unique_fields, list):
                self.entities[self.current_entity]["uniques"].append(unique_fields)
            else:
                self.entities[self.current_entity]["uniques"].append([unique_fields])
                
            print(f"  Entity {self.current_entity} has unique constraint: {unique_fields}")
    
    def _add_field_to_entity_uniques(self, field_name, field_data, key=None):
        """Add a field to the entity's unique fields tracking"""
        if not field_name:
            # We can't track this field without its name
            return
            
        if "uniques" not in self.entities[self.current_entity]:
            self.entities[self.current_entity]["uniques"] = []
            
        # If a specific key is provided, we might be adding a composite unique index
        if key and "compositeWith" in key:
            # Handle composite unique indexes
            other_fields = field_data[key]
            if isinstance(other_fields, str):
                # Single field to combine with
                self.entities[self.current_entity]["uniques"].append([field_name, other_fields])
            elif isinstance(other_fields, list):
                # Multiple fields to combine with
                self.entities[self.current_entity]["uniques"].append([field_name] + other_fields)
        else:
            # Single field unique index
            self.entities[self.current_entity]["uniques"].append([field_name])
    
    def _extract_relationships(self):
        """Extract relationships from ObjectId field references"""
        for entity_name, entity_data in self.entities.items():
            for field_name, field_data in entity_data.get("fields", {}).items():
                if isinstance(field_data, dict) and field_data.get("type") == "ObjectId":
                    # Field might be a reference to another entity
                    if field_name.endswith("Id"):
                        ref_entity = field_name[:-2]  # Remove 'Id' suffix
                        if ref_entity and ref_entity in self.entities:
                            # Add relationship
                            self.relationships.append((entity_name, ref_entity))
                            if ref_entity not in entity_data["relationships"]:
                                entity_data["relationships"].append(ref_entity)

def parse_mmd(lines):
    """
    Parse MMD schema directly
    Returns a tuple of (entities, relationships, dictionaries)
    """
    parser = SchemaParser()
    return parser.parse_mmd(lines)

def extract_entities_metadata(entities):
    """
    Extract services and inherited entities from the parsed entities
    
    Args:
        entities: Dictionary of parsed entities
        
    Returns:
        Tuple of (services, inherits) sets
    """
    services = set()
    inherits = set()
    
    for entity_name, entity_data in entities.items():
        if "inherits" in entity_data:
            for item in entity_data["inherits"]:
                if isinstance(item, str):
                    inherits.add(item)
                elif isinstance(item, dict) and "service" in item:
                    for service in item["service"]:
                        services.add(service)
    
    return services, inherits

def generate_yaml_object(entities, relationships, dictionaries, services, inherits):
    """
    Generate the output object for YAML
    
    Args:
        entities: Dictionary of entities
        relationships: List of (source, target) relationships
        dictionaries: Dictionary of dictionary definitions
        services: Set of services
        inherits: Set of inherited entities
        
    Returns:
        Output object for YAML serialization
    """
    # Prepare relationships for output
    top_relationships = []
    for source, target in relationships:
        top_relationships.append({"source": source, "target": target})
        
        # Add relationship to entity if it exists
        if source in entities:
            if "relationships" not in entities[source]:
                entities[source]["relationships"] = []
            if target not in entities[source]["relationships"]:
                entities[source]["relationships"].append(target)
    
    # Build output object
    output_obj = {
        "_relationships": top_relationships,
        "_dictionaries": dictionaries,
        "_services": list(services),
        "_inherited_entities": list(inherits),
        "_entities": entities
    }
    
    return output_obj

def convert_schema(schema_path, output_path):
    """
    Convert a schema MMD file to YAML
    
    Args:
        schema_path: Path to the schema MMD file
        output_path: Path to the output directory or file
        
    Returns:
        True if conversion was successful, False otherwise
    """
    try:
        # Configure YAML for quoted strings
        yaml.add_representer(QuotedStr, quoted_str_representer)
        
        # Read the schema file
        print(f"Reading schema from {schema_path}")
        lines = helpers.read_file_to_array(schema_path)
        
        # Parse the schema
        print("Parsing schema...")
        parser = SchemaParser()
        entities, relationships, dictionaries = parser.parse_mmd(lines)
        
        # Show parsing statistics
        print("\nParsing statistics:")
        for stat_name, count in parser.stats.items():
            print(f"  - {stat_name.capitalize()}: {count}")
        
        # Extract services and inherited entities
        services, inherits = extract_entities_metadata(entities)
        
        # Generate output object
        output_obj = generate_yaml_object(entities, relationships, dictionaries, services, inherits)
        
        # Determine output file path
        output_file = output_path
        if os.path.isdir(output_path):
            output_file = os.path.join(output_path, "schema.yaml")
        
        # Write YAML file
        print(f"Writing YAML to {output_file}")
        with open(output_file, 'w') as f:
            yaml.dump(output_obj, f, sort_keys=False, default_flow_style=False)
        
        print(f"Schema conversion completed successfully")
        return True
    
    except Exception as e:
        print(f"Error converting schema: {str(e)}")
        return False

def main():
    """Command-line entry point"""
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <schema.mmd> <output.yaml or directory>")
        return 1
    
    schema_path = sys.argv[1]
    output_path = sys.argv[2]
    
    # Support both absolute and relative paths
    schema_path_obj = Path(schema_path)
    output_path_obj = Path(output_path)
    
    if not schema_path_obj.is_absolute():
        schema_path_obj = Path.cwd() / schema_path_obj
        
    if not output_path_obj.is_absolute():
        output_path_obj = Path.cwd() / output_path_obj
    
    # Convert schema
    success = convert_schema(str(schema_path_obj), str(output_path_obj))
    
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())