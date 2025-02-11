from pathlib import Path
import re
import shlex
import sys
import helpers
import ast
import yaml

UNIQUE = "%% @unique"
VALIDATE = "%% @validate"
INHERIT = "%% @inherit"

def clean(string):
   s = string.strip()
   position = s.find(':')
   if position > 0:
      return s[:position]
   elif s.endswith(','):
      return s[:-1]
   return s

def parse(lines):
   all_entities = {}
   current_entity = None
   fields = {}
   extras = []
   relationships = []
    
   # Pattern for the entity header (e.g. "BaseEntity {")
   entity_header_pattern = re.compile(r'^(\w+)\s*\{')
   # Pattern for a field definition line: must match something like "ObjectId _id"
   field_pattern = re.compile(r'^(\w+)\s+(\w+)$')
    
   RELATION_SPEC = "||--o{"

   in_entity = False
   in_extras = False

   for line in lines:
      # Remove newline characters.
      line = line.rstrip('\n')
      stripped = line.strip()
      if not stripped:
         continue

      # If we're not inside an entity yet, look for an entity header.
      if not in_entity:
         header_match = entity_header_pattern.match(stripped)
         if header_match:
            current_entity = header_match.group(1)
            print(f"Processing entity: {current_entity}")
            in_entity = True
            # Reset fields and extras for this entity.
            fields = {}
            extras = []
         # Process relationships 
         elif stripped.find(RELATION_SPEC) > 0:
            words = stripped.split(RELATION_SPEC)
            child = words[0].strip()
            parent = clean(words[1])
            relationships.append((child, parent))
            # all_entities[parent].setdefault('children', []).append(child)
         continue

      # If the line is the closing brace, then end the entity.
      if stripped == '}':
         in_entity = False
         # Store the result in the object.
         if current_entity:
            all_entities[current_entity] = {
               "fields": fields,
               "extras": extras
            }
         current_entity = None
         in_extras = False
         continue

      # If we're inside the entity, check if we are still reading field definitions.
      if not in_extras:
         # Try to match the field pattern.
         field_match = field_pattern.match(stripped)
         if field_match:
            # It's a field definition.
            field_type = field_match.group(1)
            field_name = field_match.group(2)
            fields[field_name] = field_type
            continue
         else:
            # As soon as we encounter a line that does not match a field definition,
            # we consider that the fields are done and the rest are extras.
            in_extras = True
        
      # If in_extras, add the line (even if it starts with %% or anything else).
      if in_extras:
         extras.append(stripped)
   
   return all_entities, relationships

def process_extras(obj_dict):

   for entity, object in obj_dict.items():
      # process the extras to extract the validation, inheritance and unique data
      inherits = []
      validations = {}
      uniques = []
      for line in object["extras"]:
        if line.startswith(INHERIT):
           inherits.append( process_inheritance(line) ) 
        elif line.startswith(VALIDATE):
          field, validation = process_validation(line)
          validations[field] = validation
        elif line.startswith(UNIQUE):
           uniques.append( {'fields': process_unique(line, uniques) } )

      del obj_dict[entity]["extras"]
      if len(validations) > 0:
         obj_dict[entity]["validations"] = validations
      if len(inherits) > 0:
          obj_dict[entity]["inherits"] = inherits
      if len(uniques) > 0:
          obj_dict[entity]["uniques"] = uniques
 
      

def process_validation(line):
   field_validations = {}
   lexer = shlex.shlex(line, posix=True)
   lexer.whitespace = ' '
   lexer.whitespace_split = True
   tokens = list(lexer)
   if tokens[3] == '{' and tokens[-1] == '}':
      field = clean(tokens[2])
               
   # Get validations from the token list
      i = 4
      while(i > 0 and tokens[i] != '}'):
         key, value, i = get_validation(i, tokens)
         if i > 0:
            field_validations[key] = value
   return field, field_validations


def get_validation(i, tokens):
   attribute = clean(tokens[i])
   if attribute != 'enum':
      value = clean(tokens[i+1])
      return attribute, value, i + 2
   else:
      start = i + 1
      i = start
      words = []
      while i < len(tokens) and tokens[i] != '}':
         word = clean(tokens[i])
         words.append(word[1:] if word.startswith("[") else word[:-1] if word.endswith("]") else word)
         if tokens[i].endswith(']'):
            value = repr(words)
            return attribute, value, i + 1
         else:
            i += 1
      return '', '', -1

# Handle '%% @inherit BaseEntity'
def process_inheritance(line):
   line = line[len(INHERIT):]
   return line.strip()

# Handle '%% @unique email', or '%% @unique email, phone'
def process_unique(line, uniques):
   line = line[len(UNIQUE):]
   return split_strip(line, '+')

def split_strip(line, sep=','):
   return [word.strip() for word in line.split(sep) if word.strip()]


import ast
import yaml

def convert_validation_value(key, value):
    """
    Convert a validation value from a string to an appropriate type.
    
    - For boolean keys ("required", "autoGenerate", "autoUpdate"), convert "true"/"false" to booleans.
    - For numeric keys ("minLength", "maxLength"), attempt to convert to an integer.
    - For "enum", if the value is a string that looks like a list (i.e. starts with '[' and ends with ']'),
      use ast.literal_eval to convert it to a list of strings.
    - Otherwise, return the value unchanged.
    """
    bool_keys = {"required", "autoGenerate", "autoUpdate"}
    numeric_keys = {"minLength", "maxLength"}
    
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

def generate_schema_yaml(entities, relationships, filename):
    """
    Generates YAML output from the given entities dictionary and relationships list,
    and writes the YAML to the specified filename.
    
    Arguments:
      entities: a dictionary produced by your parser. Each key is an entity name, and its value is a dictionary
                with at least:
                   - "fields": a dictionary mapping field names to field types.
                   - "validations": a dictionary mapping field names to validation rules.
                   - Optionally, "inherits": a list of parent entity names.
      relationships: a list of tuples (child, parent) representing relationships.
                     (For example: [('Account', 'User'), ('User', 'Profile'), ('Profile', 'TagAffinity'),
                                    ('UserEvent', 'User'), ('UserEvent', 'Event'), ('Url', 'Crawl')])
      filename: the output filename for the YAML.
    
    This function:
      1. Merges validations into each field's definition (under "fields")—converting booleans and numeric values as needed.
      2. Removes the separate "validations" section.
      3. Populates each entity’s "relations" array using the provided relationships.
      4. Builds a top-level _relationships list.
      5. Writes the final output object as YAML.
    """
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
        if "relations" not in entity_data:
            entity_data["relations"] = []
    
    # Process relationships: build top-level _relationships and update each entity's "relations".
    top_relationships = []
    for child, parent in relationships:
        top_relationships.append({"source": child, "target": parent})
        if child in entities:
            if "relations" not in entities[child]:
                entities[child]["relations"] = []
            if parent not in entities[child]["relations"]:
                entities[child]["relations"].append(parent)
    
    # Construct final output object.
    output_obj = {"_relationships": top_relationships}

    for entity_name, entity_data in entities.items():
        # output Inheritance
        if "inherits" in entity_data and not isinstance(entity_data["inherits"], list):
            entity_data["inherits"] = [entity_data["inherits"]]
    
        output_obj[entity_name] = entity_data
    
    with open(filename, "w") as f:
        yaml.dump(output_obj, f, sort_keys=False)

    print(f"Schema written to {filename}")

if __name__ == "__main__":
   if len(sys.argv) == 3:
      infile = sys.argv[1]
      outfile = Path(sys.argv[2], "schema.yaml")
   else:
      print(f"Usage: python {sys.argv[0]} <schema.mmd> <output_dir>")
      sys.exit(1)

   lines = helpers.read_file_to_array(infile)

   obj_dict, relationships = parse(lines)

   process_extras(obj_dict)

   generate_schema_yaml(obj_dict, relationships, outfile)
