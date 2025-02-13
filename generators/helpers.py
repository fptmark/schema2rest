import os
from pathlib import Path
import sys
from typing import Type

import yaml

def generate_file(path_root: str, file_name: Path, lines)-> Path:
    outfile = Path(path_root) / file_name
    outfile.parent.mkdir(parents=True, exist_ok=True)
    with open(outfile, "w") as main_file:
        if isinstance(lines, str):
            main_file.write(lines)
        else: 
            main_file.writelines(lines) 
    return outfile


def read_file_to_array(template: str, num=0)-> list[str]:
    """
    Reads the content of a file and returns it as an array of strings.

    Args:
        file_name (str): The name or path of the file to be read.

    Returns:
        list[str]: A list of strings, where each string is a line in the file.
    """
    try:
        file_name = f"{template}{num}.txt" if num > 0 else template
        with open(file_name, 'r', encoding='utf-8') as file:
            return file.readlines()
    except FileNotFoundError:
        print(f"Error: The file '{file_name}' was not found.")
        return []
    except IOError as e:
        print(f"Error reading the file '{file_name}': {e}")
        return []

# Example usage:
#file_content = read_file_to_array('example.txt')
# Function to convert plural entity names to singular (basic rule-based approach)

def singularize(name):
    if name.endswith("ies"):  # e.g., categories → category
        return name[:-3] + "y"
    elif name.endswith("s") and not name.endswith("ss"):  # e.g., users → user (but not addresses)
        return name[:-1]
    return name  # Default: leave it unchanged

RESERVED_TYPES = {"ISODate", "ObjectId", "_relationships"}
def get_schema(schema_path: str, reserved_types=RESERVED_TYPES)-> dict:
    with open(schema_path, "r") as file:
        schema = yaml.safe_load(file)

    # Extract entity schemas, skipping reserved types and metadata keys
    entity_schemas = {
        name: details for name, details in schema.items() if name not in reserved_types and isinstance(details, dict)
    }
    # print(f"{entity_schemas}")#['name']}")
    return entity_schemas

def pluralize(name) -> str:

    if name.endswith("y"):  # e.g., category → categories
        return name[:-1] + "ies"
    return name + "s"  # Default: just add "s"

#
# Handle script management for updating indexes
#
def generate_index_script_content(schema):
    """
    Given the schema (a dict loaded from YAML), generate a shell script
    that creates indexes for each unique constraint.
    The generated script does not embed a specific db name; instead, it expects the
    database name as a command-line argument.
    """
    lines = []
    # Header: do not use schema's dbName, but get it from config.json
    header = (
        "#!/bin/sh\n"
        "if [ ! -f config.json ]; then\n"
        "  echo \"config.json not found\"\n"
        "  exit 1\n"
        "fi\n"
        "MONGO_URI=$(jq -r '.mongo_uri' config.json)\n"
        "DB_NAME=$(jq -r '.db_name' config.json)\n\n"
    )
    lines.append(header)
    
    # Iterate over each entity (skip keys starting with '_' such as _relationships)
    for entity, data in schema.items():
        if entity.startswith("_"):
            continue
        uniques = data.get("uniques", [])
        # For each unique constraint, generate an index creation command.
        for unique in uniques:
            fields = unique.get("fields", [])
            if fields:
                # Build a comma-separated list in the form: "field": 1
                key_str = ", ".join([f'"{field}": 1' for field in fields])
                # Generate a mongo shell command that uses the runtime variable DB_NAME.
                cmd = f'mongo $MONGO_URI/$DB_NAME --eval \'db.{entity.lower()}.createIndex({{{key_str}}}, {{unique:true}})\''
                lines.append(cmd)
                lines.append('\n')
    return "\n".join(lines)

def update_index_script(schema, script_path="index.sh"):
    """
    Updates the index script based on the provided schema.

    Workflow:
      1) If index.sh exists, move it to index.sh.old.
      2) Generate a new index.sh using generate_index_script_content(schema).
      3) Compare the new vs old script; if identical, delete the new script.
         If different, output a message indicating that an index change was detected.
    """
    # Step 1: Backup existing script if it exists.
    if os.path.exists(script_path):
        backup_path = script_path + ".old"
        os.replace(script_path, backup_path)
        print(f"Existing {script_path} moved to {backup_path}")

    # Step 2: Generate new index script content.
    new_script_content = generate_index_script_content(schema)
    with open(script_path, "w") as f:
        f.write(new_script_content)
    # Make the script executable.
    os.chmod(script_path, 0o755)
    print(f"New index script generated at {script_path}")

    # Step 3: Compare with backup if available.
    backup_path = script_path + ".old"
    if os.path.exists(backup_path):
        with open(backup_path, "r") as f_old:
            old_content = f_old.read()
        with open(script_path, "r") as f_new:
            new_content = f_new.read()
        if old_content == new_content:
            os.remove(script_path + ".old")
            print("No changes in indexes detected; backup script discarded.")
        else:
            print("*** Index change detected - run index.sh")
    else:
        print("*** Index script generated. Run index.sh")
