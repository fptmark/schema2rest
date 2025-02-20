from pathlib import Path
import re
import shlex
import sys
import helpers
import ast
import yaml
from typing import Any

DICTIONARY = "%% @dictionary"
UNIQUE = "%% @unique"
VALIDATE = "%% @validate"
INHERIT = "%% @inherit"
SERVICE = "%% @service"

### Define a custom formatter for quoting strings in the YAML output
class QuotedStr(str):
    pass

def quoted_str_representer(dumper, data):
    return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='"')

yaml.add_representer(QuotedStr, quoted_str_representer)

def parse(lines):
    all_entities = {}
    current_entity = None
    fields = {}
    decorations = []
    relationships = []
    dictionaries = {}
    
    # Pattern for the entity header (e.g. "BaseEntity {")
    entity_header_pattern = re.compile(r'^(\w+)\s*\{')
    # Pattern for a field definition line: must match something like "ObjectId _id"
   #  field_pattern = re.compile(r'^(\w+)\s+(\w+)$')
    
    RELATION_SPEC = "||--o{"

    in_entity = False

    for line in lines:
        line = line.rstrip('\n')
        stripped = line.strip()
        if not stripped:
            continue

        # Process dictionary lines
        if stripped.startswith(DICTIONARY):
            lexer = shlex.shlex(stripped, posix=True)
            lexer.whitespace = ' '
            lexer.whitespace_split = True
            tokens = list(lexer)
            if tokens[3] == '{' and tokens[-1] == '}':
                if tokens[2] in dictionaries:
                    dictionary = dictionaries[tokens[2]]
                else:
                    dictionary = {}
                    dictionaries[tokens[2]] = dictionary
                for i in range(4, len(tokens) - 2, 2):
                    value = tokens[i+1]
                    value = value[:-1] if value.endswith(',') else value
                    attribute = ''.join(filter(str.isalpha, tokens[i]))
                    dictionary[attribute] = QuotedStr(value)
            continue

        # Look for an entity header.
        if not in_entity:
            header_match = entity_header_pattern.match(stripped)
            if header_match:
                current_entity = header_match.group(1)
                print(f"Processing entity: {current_entity}")
                in_entity = True
                fields = {}
                decorations = []
            elif stripped.find(RELATION_SPEC) > 0:
                words = stripped.split(RELATION_SPEC)
                child = words[0].strip()
                parent = helpers.clean(words[1])
                relationships.append((child, parent))
            continue

        # End of entity.
        if stripped == '}':
            in_entity = False
            if current_entity:
                all_entities[current_entity] = {
                    "fields": fields,
                    "decorations": decorations
                }
            current_entity = None
            continue

        # Process fields or decorations
        if stripped.startswith('%% @'):  
            decorations.append(stripped)
        else:
            words = stripped.split()
            if len(words) >= 2:
                field_type = words[0]
                field_name = words[1]
                fields[field_name] = field_type

                # check for inline validation
                if len(words) > 6 and f'{words[2]} {words[3]}' == VALIDATE:
                   # Add validation to decorations
                   decorator = f"{VALIDATE} {field_name}: {' '.join(words[4:])}"
                   decorations.append(decorator)
                continue
    
    return all_entities, relationships, dictionaries

def process_entity_decorations(obj_dict, dictionaries) -> tuple[dict[str, Any], set]:
    all_services = {}  # global list of services
    all_inherits = set()  # global list of inherited entities

    for entity, obj in obj_dict.items():
        inherits = []
        validations = {}
        uniques = []
        services = []

        for line in obj.get("decorations", []):
            line = line.strip()
            if line.startswith(INHERIT):
                inherit = process_inheritance(line)
                inherits.append(inherit)
                all_inherits.add(inherit)
            elif line.startswith(VALIDATE):
                field, validation = process_validation(line, dictionaries)
                validations[field] = validation
            elif line.startswith(UNIQUE):
                uniques.append({'fields': process_unique(line)})
            elif line.startswith(SERVICE):
                service, params = process_service(line)
                services.append(service)
                all_services[service] = params

        if "decorations" in obj:
            del obj["decorations"]

        if validations:
            obj["validations"] = validations

        # Add services into inheritance as a mapping.
        if services:
            inherits.append({"service": services})
        if inherits:
            obj["inherits"] = inherits

    return all_services, all_inherits

def process_validation(line, dictionaries):
    field_validations = {}
    lexer = shlex.shlex(line, posix=True)
    lexer.whitespace = ' '
    lexer.whitespace_split = True
    tokens = list(lexer)
    if tokens[3] == '{' and tokens[-1] == '}':
        field = helpers.clean(tokens[2])
        i = 4
        while i < len(tokens) and tokens[i] != '}':
            key, value, i = get_validation(i, tokens)
            if i > 0:
                # Check if value is a dictionary key
                if value.startswith("dictionary="):
                    words = value[len("dictionary="):].split('.')
                  #   value = dictionaries.get(words[0], {}).get(words[1], value)
                    if words[1] in dictionaries[words[0]]:
                        value = dictionaries[words[0]][words[1]]
                    else:
                        print(f"Error: dictionary key '{value}' not found.")
                field_validations[key] = value
    return field, field_validations

def get_validation(i, tokens):
    attribute = helpers.clean(tokens[i])
    if attribute != 'enum':
        value = helpers.clean(tokens[i+1])
        return attribute, value, i + 2
    else:
        start = i + 1
        i = start
        words = []
        while i < len(tokens) and tokens[i] != '}':
            word = helpers.clean(tokens[i])
            words.append(word[1:] if word.startswith("[") else word[:-1] if word.endswith("]") else word)
            if tokens[i].endswith(']') or tokens[i].endswith("],"):
                value = repr(words)
                return attribute, value, i + 1
            else:
                i += 1
        return '', '', -1

def process_inheritance(line):
    return line[len(INHERIT):].strip()

def process_unique(line):
    line = line[len(UNIQUE):]
    return helpers.split_strip(line, ',')

def process_service(line) -> tuple[str, dict]:
    words = line[len(SERVICE):].strip().split(' ')
    obj = helpers.process_object_line(words[1:])
    return words[0], obj

def convert_validation_value(key, value):
    bool_keys = {"required", "autoGenerate", "autoUpdate"}
    numeric_keys = {"minLength", "maxLength", "min", "max", "lt", "gt", "lte", "gte"}
    
    if key in bool_keys:
        if isinstance(value, str):
            return True if value.lower() == "true" else False
        return value
    elif key in numeric_keys:
        try:
            return int(value)
        except Exception:
            return value
    elif key == "enum":
        if isinstance(value, str) and value.startswith('[') and value.endswith(']'):
            try:
                evaluated = ast.literal_eval(value)
                if isinstance(evaluated, list):
                    return [str(item) for item in evaluated]
                return evaluated
            except Exception:
                return value
        return value
    else:
        return value

def generate_schema_yaml(entities, relationships, dictionaries, services, inherits, filename):
    # Merge validations into fields.
    for entity_name, entity_data in entities.items():
        fields = entity_data.get("fields", {})
        validations = entity_data.get("validations", {})
        for field_name, field_type in fields.items():
            merged = {"type": field_type}
            if field_name in validations:
                for key, val in validations[field_name].items():
                    merged[key] = convert_validation_value(key, val)
            fields[field_name] = merged

        if "validations" in entity_data:
            del entity_data["validations"]
         
        # Ensure relationships is always initialized.
        entity_data["relationships"] = []
    
    # Process relationships.
    top_relationships = []
    for child, parent in relationships:
        top_relationships.append({"source": child, "target": parent})
        if child in entities:
            if parent not in entities[child]["relationships"]:
                entities[child]["relationships"].append(parent)
    
    output_obj = {
        "_relationships": top_relationships,
        "_dictionaries": dictionaries,
        "_services": services,
        "_inherited_entities": inherits,
        "_entities": entities
    }

    with open(str(filename), "w") as f:
        yaml.dump(output_obj, f, sort_keys=False, default_flow_style=False)

    print(f"Schema written to {filename}")

def convert_schema(schema_path, output_dir):
    yaml.add_representer(QuotedStr, quoted_str_representer)
    lines = helpers.read_file_to_array(schema_path)
    obj_dict, relationships, dictionaries = parse(lines)
    services, inherits = process_entity_decorations(obj_dict, dictionaries)
    generate_schema_yaml(obj_dict, relationships, dictionaries, services, list(inherits), output_dir)

if __name__ == "__main__":
    if len(sys.argv) == 3:
        infile = sys.argv[1]
        outfile = Path(sys.argv[2], "schema.yaml")
    else:
        print(f"Usage: python {sys.argv[0]} <schema.mmd> <output_dir>")
        sys.exit(1)

    convert_schema(infile, outfile)
