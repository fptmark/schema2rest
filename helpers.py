from pathlib import Path
from typing import Type
from pymongo.errors import DuplicateKeyError
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


def singularize(name):
    if name.endswith("ies"):  # e.g., categories → category
        return name[:-3] + "y"
    elif name.endswith("s") and not name.endswith("ss"):  # e.g., users → user (but not addresses)
        return name[:-1]
    return name  # Default: leave it unchanged

def pluralize(name) -> str:

    if name.endswith("y"):  # e.g., category → categories
        return name[:-1] + "ies"
    return name + "s"  # Default: just add "s"
#


