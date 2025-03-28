from pathlib import Path
import sys
import traceback
import yaml
import os
from typing import Dict, Set, Any, List, Tuple
from decorators import Decorator


### Define a custom formatter for quoting strings in the YAML output 
class QuotedStr(str):
    """String that will be quoted in YAML output"""
    pass

def quoted_str_representer(dumper, data):
    """Custom YAML representer for quoted strings"""
    return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='"')

yaml.add_representer(QuotedStr, quoted_str_representer)

# Avoid YAML aliases (anchors) in the output
class NoAliasDumper(yaml.SafeDumper):
    def ignore_aliases(self, data):
        return True

class SchemaParser:
    """Parser for MMD schema files"""
    
    def __init__(self):
        self.entities = {}
        self.abstractions = {}
        self.relationships = []
        self.current_entity = None
    
    def parse_mmd(self, file_name):
        """
        Parse MMD schema directly
        Returns a tuple of (entities, relationships, dictionaries)
        """
        with open(file_name, 'r', encoding='utf-8') as file:
            lines = file.readlines()

        print("Starting schema parsing...")
        
        decorator = Decorator(self.entities)  # Create the decorator processor

        for line in lines:
            line = line.strip()
            
            # Skip empty lines
            if not line:
                continue
            
            # Entity definition
            if self._is_entity_definition(line):
                self._handle_entity_definition(line)
            
            # Entity end
            elif line == "}":
                self.current_entity = None
            
            # Skip if not in entity or just a comment (not decorator)
            elif self.current_entity:
                if line[0:2] == '%%':
                    if decorator.has_decorator(line):
                        decorator.process_decorations(line, self.current_entity)

                else:
                    words = line.split(' ')
                    if len(words) >= 2:
                        field_name = words[1]
                        field_type = words[0]
                        self.entities[self.current_entity]["fields"][field_name] = { "type": field_type }
                        if decorator.has_decorator(line):
                            decorator.process_decorations(line, self.current_entity, field_name, field_type)

            elif decorator.has_decorator(line):
                decorator.process_decorations(line, self.current_entity)
                
            # Process relationships
            elif "||--o{" in line:
                words = line.split("||--o{")
                source = words[0].strip()
                pos = words[1].find(":")
                target = words[1][:pos].strip() if pos >= 0 else words[1].strip()
                self.relationships.append((source, target))

                # Auto add a foreign key into each entity based on a relationship - this is now handled by the model generator
                # entity = self.entities[target]
                # entity["fields"][source + "Id"] = { "type": "ObjectId", "required": 'true' }


        dictionaries = decorator.get_objects()

        # Extract relationships from field references
        # self._extract_relationships()
        
        # Apply decorations from the decorator processor
        # self._apply_decorations()
        
        return self.entities, self.relationships, dictionaries
        
    
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
    
        
    
    # def _extract_relationships(self):
    #     """Extract relationships from ObjectId field references"""
    #     for entity_name, entity_data in self.entities.items():
    #         for field_name, field_data in entity_data.get("fields", {}).items():
    #             if isinstance(field_data, dict) and field_data.get("type") == "ObjectId":
    #                 # Field might be a reference to another entity
    #                 if field_name.endswith("Id"):
    #                     ref_entity = field_name[:-2]  # Remove 'Id' suffix
    #                     if ref_entity and ref_entity in self.entities:
    #                         # Add relationship
    #                         self.relationships.append((entity_name, ref_entity))
    #                         if ref_entity not in entity_data["relationships"]:
    #                             entity_data["relationships"].append(ref_entity)

def parse_mmd(lines):
    """
    Parse MMD schema directly
    Returns a tuple of (entities, relationships, dictionaries)
    """
    parser = SchemaParser()
    return parser.parse_mmd(lines)

def extract_entities_metadata(entities):
    """
    Extract services and included entities from the parsed entities
    
    Args:
        entities: Dictionary of parsed entities
        
    Returns:
        Tuple of (services, includes) sets
    """
    services = set()
    includes = set()
    
    for entity_name, entity_data in entities.items():
        if "include" in entity_data:
            for item in entity_data["include"]:
                if isinstance(item, str):
                    includes.add(item)
        if "service" in entity_data:
            for item in entity_data["service"]:
                if isinstance(item, str):
                        services.add(item)
    
    return services, includes

def generate_yaml_object(entities, relationships, dictionaries, services, includes):
    """
    Generate the output object for YAML
    
    Args:
        entities: Dictionary of entities
        relationships: List of (source, target) relationships
        dictionaries: Dictionary of dictionary definitions
        services: Set of services
        includes: Set of abstract entities
        
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
        "_included_entities": list(includes),
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
        
        # Parse the schema
        print("Parsing schema...")
        parser = SchemaParser()
        entities, relationships, dictionaries = parser.parse_mmd(schema_path)
        
        # Extract services and inherited entities
        services, includes = extract_entities_metadata(entities)
        
        # Generate output object
        output_obj = generate_yaml_object(entities, relationships, dictionaries, services, includes)
        
        # Determine output file path
        output_file = output_path
        if os.path.isdir(output_path):
            output_file = os.path.join(output_path, "schema.yaml")
        
        # Write YAML file
        print(f"Writing YAML to {output_file}")
        with open(output_file, 'w') as f:
            yaml.dump(output_obj, f, sort_keys=False, default_flow_style=False, Dumper=NoAliasDumper)
        
        print(f"Schema conversion completed successfully")
        return True
    
    except Exception as e:
        print(f"Error converting schema: {str(e)}")
        traceback.print_exc()
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
    ret = main()
    # sys.exit(ret)