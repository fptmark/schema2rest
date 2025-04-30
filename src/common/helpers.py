import os
from pathlib import Path
from itertools import takewhile
import re
from typing import List, Tuple, Dict

def load_templates(dirpath: str) -> List[Tuple[str,str]]:
    """
    Return a list of (filename, content) for each .tpl file in dirpath
    whose name begins with a number, sorted by that leading number.
    """
    files = []
    for f in Path(dirpath).glob("*.tpl"):
        m = re.match(r"^(\d+)", f.name)
        if m:
            files.append((int(m.group(1)), f))
    files.sort(key=lambda x: x[0])
    return [(f.name, f.read_text()) for _, f in files]

_placeholder_re = re.compile(r"\{([^{}]+)\}")

def render_template_content(
    template: str,
    vars_dict: Dict[str,str]
) -> str:
    """
    Substitute every {key} in `template` with vars_dict[key] if present.
    If key not in vars_dict:
      - If the line is *only* "{key}", skip that line altogether.
      - Otherwise, leave the "{key}" text intact.
    Returns the full rendered text (with skipped lines removed).
    """
    out_lines = []
    for line in template.splitlines():
        # find all placeholders in this line
        placeholders = _placeholder_re.findall(line)
        if not placeholders:
            # no placeholders at all → keep the line
            out_lines.append(line)
            continue

        new_line = line
        skip = False

        for key in placeholders:
            token = f"{{{key}}}"
            if key in vars_dict:
                # perform substitution
                new_line = new_line.replace(token, vars_dict[key])
            else:
                # missing var: if the *entire* line is exactly "{key}", we skip it
                if new_line.strip() == token:
                    skip = True
                    break
                # else: leave token intact

        if not skip:
            out_lines.append(new_line)

    return "\n".join(out_lines)


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

def clean(string):
    s = string.strip()
    position = s.find(':')
    if position > 0:
        return s[:position]
    elif s.endswith(','):
        return s[:-1]
    return s

def process_object_line(words):
    obj = {}
    i = 0
    words = get_until_hash(words)
    if len(words) >= 4 and words[0] == '{' and words[-1] == '}':
        for i in range(1, len(words)-1, 2):
            key = clean(words[i])
            value = clean(words[i+1])
            value = value[:-1] if value.endswith(',') else value
            obj[key] = value
    return obj

def get_until_hash(strings: list[str]) -> list[str]:
    return list(takewhile(lambda s: not s.startswith('#'), strings))

def split_strip(line, sep=','):
    return [word.strip() for word in line.split(sep) if word.strip()] 

def valid_backend(backend: str) -> bool:
    """
    Validate the backend name.
    """
    valid_backends = ['mongo', 'es']
    if not backend in valid_backends:
        print(f"Invalid backend '{backend}'. Valid backends are: {', '.join(valid_backends)}")
        return False
    else:
        return True

def write(path_root: str, backend: str, file_name: str, lines: str | List[str]):
    if isinstance(lines, List):
        lines = "\n".join(lines)

    dir = Path(path_root) / backend / "app"
    dir.mkdir(parents=True, exist_ok=True)
    
    with open(dir / file_name, "w") as f:
        f.write(lines)
    
    print(f"Generated {dir}/{file_name} successfully.")