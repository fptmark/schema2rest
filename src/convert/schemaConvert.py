from copy import deepcopy
import os
from pathlib import Path
import sys
import traceback
import json5
import yaml
from typing import Dict, Set, Any, List, Tuple
from src.convert.decorators import Decorator, ABSTRACT


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

# take a decoration string, return the start of json doc
def get_json_decoration(decor: str, delim: str = '{') -> Tuple[str, str, Any]:
    start = decor.find(delim)
    end = start + 1
    while end <= len(decor):
        try:
            obj = json5.loads(decor[start:end])
            tail = decor[end:].removesuffix(',').removeprefix(',')
            words = tail.split(maxsplit=1)
            if len(words) == 0:
                return '', '', obj
            elif len(words) == 1:
                return words[0], '', obj
            else:
                return words[0], words[1], obj
        except:
            end = end + 1
    print(f"FATAL ERROR in decoration {decor}")
    exit(1)

class SchemaParser:
    """Parser for MMD schema files"""
    
    def __init__(self):
        self.entities = {}
        self.abstractions = {}
        self.relationships: List[Tuple[str, str]] = []
        self.dictionaries = {}
        self.current_entity = None
        self.service_definitions = {}
        self.services = []
    
    def parse_mmd(self, path):
        with open(path / "schema.mmd", 'r', encoding='utf-8') as file:
            lines: List[str] = []
            for line in file.readlines():
                if line and line.strip():
                    # cleanup lines
                    line = line.strip().replace('%%@', '%% @')
                    if line == "erDiagram":
                        continue
                    lines.append(line)

        print("Starting schema parsing...")

        # field decorators include @validate, @unique, @ui on a field defn line or a line @ui <fieldname>
        # entity decorators include @ui, @include, @service, @operations, @unique x + y (composites only)
        relationships: List[Tuple[str, str]] = []

        self.load_services(path)

        print("Pass 1 - processing dictionaries...")
        self.extract_dictionary_entries(lines)

        print("Pass 2 - processing relationships...")
        self.extract_relationships(lines)

        print("Pass 3 - processing entities and fields...")
        self.extract_entity_definitions(lines)  # includes abstract types and files with field decorator map

        self.add_relationships()   # add objects to entities to resolve fk relationsips
        self.add_abstracts()                    # add abstract entities to concrete entities
        
        print("Pass 4 - processing entity decorations...")
        self.process_entity_decorations()        # process field and entity level decorators


    def validate_service(self, entity: str, svc_name: str, svc_details: Dict[str, Any]) -> bool:
        service_def = self.service_definitions.get(svc_name, None) # type: ignore
        if service_def:
            for field in service_def.get("requiredFields", {}):
                if field not in svc_details.get('fields', {}):
                    print(f"FATAL ERROR: Service {svc_name} for entity missing required field {field}")
                    exit(1)
            print(f"Validated service {svc_name} for {entity}")
            self.services.append(svc_name)
        # validate service details here
        return True

    def load_services(self, path: Path):
        services_path = path / "app" /"services" / "services_registry.json"
        self.service_definitions = json5.loads(services_path.read_text(encoding='utf-8'))

    def extract_entity_definitions(self, lines):
        entity = None
        for line in lines:
            words = line.split()
            if entity is None:
                if words[1] == '{' and (len(words) == 2 or words[2] == '%%'):
                    entity = self.entities.setdefault(words[0], {})
                    entity.setdefault('decorators', [])
                    entity.setdefault('fields', {})
                    print(f" >>> Processing entity: {words[0]}")
            elif line == '}':
                entity = None
            elif words[0] == '%%':  # entity level decorator but may be a field decorator defined at the entity level
                if words[1] == '@abstract':
                    entity['abstract'] = True
                else:
                    entity['decorators'].append(' '.join(words[1:]))
            else:
                field_name = words[1]
                field_type = words[0]
                entity['fields'][field_name] = { "type": field_type }
                if len(words) > 2 and words[2] == '%%':    # decoration on field line
                    entity['fields'][field_name]['decorators'] =' '.join(words[3:])
                    

    def process_field_decorations(self, decoration: str, entity: str, field: str, decor_obj_start: str):
        e = self.entities[entity]
        f = e['fields'][field]
        if decor_obj_start:
            next_decorator, decor_obj_start, obj = get_json_decoration(decor_obj_start)
        else:
            next_decorator = None
        while True:
            if decoration == '@ui':
                f.setdefault('ui', {}).update(obj)
            elif decoration == '@unique':
                e.setdefault('unique', []).append([field])
            elif decoration == '@validate':
                f.update(obj)

            if next_decorator:
                self.process_field_decorations(next_decorator, entity, field, decor_obj_start)
            return
            

    def process_entity_decorations(self):
        for entity in self.entities.keys():
            for decorator in self.entities[entity]['decorators']:
                self.process_entity_decoration(entity, decorator)
            self.entities[entity].pop('decorators', None)
            for field_name, field in self.entities[entity]['fields'].items():
                words = field.get('decorators', '').split(maxsplit=1)
                if len(words) >= 2:
                    self.process_field_decorations(words[0], entity, field_name, words[1])
                self.entities[entity]['fields'][field_name].pop('decorators', None)


    def process_entity_decoration(self, entity: str, decorator: str):
        # find json for decoration
        words = decorator.split(' ', maxsplit=2)
        e = self.entities[entity]

        # ui field decorators may also exist at the entity level
        if words[0] == '@ui' and words[2].startswith('{'):
            self.process_field_decorations('@ui', entity, words[1], words[2])

        elif words[0] == '@ui' and words[1] == '{':    # entity ui decorations
            _, _, ui_decor = get_json_decoration(decorator)
            e['ui'] = ui_decor

        # handle entity level decorators only - ui, unique, service, operations MAYBE include too?
        elif words[0] == '@unique':
            _, _, uniques = get_json_decoration(decorator, delim='[')
            e.setdefault('unique', []).append(uniques)

        elif words[0] == '@operations':
            _, _, operations = get_json_decoration(decorator, delim='[')
            e['operations'] = ''.join([op[:1] for op in operations])

        elif words[0] == '@service':
            _, _, services = get_json_decoration('{' + words[2], delim='{')
            for svc_name, svc_details in services.items():
                if self.validate_service(entity, svc_name, svc_details):
                    e['services'] = {svc_name: svc_details}


    def extract_dictionary_entries(self, lines):
        for line in lines:
            words = line.split()
            if words[0] == '%%' and words[1] == '@dictionary':
                dict_name = words[2]
                dictionary_text = ' '.join(words[3:])
                dict_content = json5.loads(dictionary_text)

                # Store in class variables
                if isinstance(dict_content, dict):
                    self.dictionaries.setdefault(dict_name, {}).update(dict_content)


    def extract_relationships(self, lines):
        for line in lines:
            if "||--o{" in line:
                words = line.split("||--o{")
                source = words[0].strip()
                pos = words[1].find(":")
                target = words[1][:pos].strip() if pos >= 0 else words[1].strip()
                self.relationships.append((source, target))
        return 


    def add_relationships(self):
        for source, dest in self.relationships:
            field_name = f"{source.lower()}Id"
            field_def = self.entities[dest]["fields"].setdefault(field_name, {})
            field_def["type"] = "ObjectId"
            field_def["required"] = True


    def add_abstracts(self):
        for entity in self.entities.keys():
            for decorator in self.entities[entity]['decorators']:
                if decorator.startswith('@include'):
                    _, _, includes = get_json_decoration(decorator, delim='[')
                    if isinstance(includes, List):
                        for abstract in includes:
                            for name, defn in self.entities[abstract]['fields'].items():
                                obj = deepcopy(defn)
                                obj.setdefault('ui', {})['displayAfterField'] = '-1'
                                self.entities[entity]['fields'][name] = obj


def generate_yaml_object(entities, relationships, dictionaries, services): #, includes):
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
    # Build output object
    output_obj = {
        # "_relationships": top_relationships,
        "_dictionaries": dictionaries,
        "_services": services,
        "_entities": entities
    }
    
    return output_obj

def convert_schema(schema_path: Path):
    """
    Convert a schema MMD file to YAML
    
    Args:
        schema_path: Path to the schema MMD file
        backend: Backend type (e.g., "mongo")
        
    Returns:
        yaml file if conversion was successful, None otherwise
    """
    try:
        # Configure YAML for quoted strings
        yaml.add_representer(QuotedStr, quoted_str_representer)
        
        # Read the schema file
        print(f"Reading schema from {schema_path}")
        
        # Parse the schema
        print("Parsing schema...")
        parser = SchemaParser()
        parser.parse_mmd(schema_path)
        output_obj = generate_yaml_object(parser.entities, parser.relationships, parser.dictionaries, parser.services) #, includes)
        
        # Determine output file path
        output_file = schema_path / "schema.yaml"
        
        # Write YAML file
        print(f"Writing YAML to {output_file}.  Generated {len(parser.entities)} entities")
        with open(output_file, 'w') as f:
            yaml.dump(output_obj, f, sort_keys=False, default_flow_style=False, Dumper=NoAliasDumper)
        
        print(f"Schema conversion completed successfully")
        return output_file if len(parser.entities) else None
    
    except Exception as e:
        print(f"Error converting schema: {str(e)}")
        traceback.print_exc()
        return None

if __name__ == "__main__":
    success = None

    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <build_path>")
        exit(1)
    
    # Support both absolute and relative paths
    schema_path_obj = Path(sys.argv[1])
    
    if not schema_path_obj.is_absolute():
        schema_path_obj = Path.cwd() / schema_path_obj
        
    # Convert schema
    success = convert_schema(schema_path_obj)
    
    exit(0 if success else 1)
