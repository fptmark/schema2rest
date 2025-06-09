#!/usr/bin/env python3
import re
from typing import Dict, List, Mapping, Union
from pathlib import Path

PLACEHOLDER_PATTERN = re.compile(r"\{\{(\w+)\}\}")

class Templates:

    templates: dict[str, list[str]] = {}

    # base_dir is the path where the template directory exists
    def __init__(self, base_dir: Path, component: str):
        self.templates = {}
        template_dir = Path(base_dir) / "templates" / component
        names = []
        for fn in template_dir.iterdir():
            if fn.suffix == ".tpl":
                name = fn.name[:-len(fn.suffix)]
                names.append(name)
                self.templates[name] = fn.read_text().splitlines()
    
    def list(self) -> List[str]:
        """
        List all available templates.
        """
        return sorted(self.templates.keys())

    def _get_template(self, tpl_name: str) -> List[str]:
        for name in self.templates:
            if name.startswith(tpl_name):
                tpl_name = name
                break
        if tpl_name not in self.templates:
            raise RuntimeError(f"Template '{tpl_name}' not found")
        return self.templates[tpl_name]
    
    def render( self, tpl_name: str, vars_map: Mapping[str, Union[str, List[str]]],) -> List[str]:
        """
        Single-pass {{Key}}→vars_map[Key] substitution.
        - Error if tpl_name references a var not in vars_map.
        - If a line is exactly '{{Key}}', the value may be a string or list:
          • empty or empty list → skip line
          • non-empty string  → split on '\n' and emit each non-empty line
          • list of strings   → emit each non-empty item
        - Inline placeholders (with other text on the line) expect only strings.
        """
        lines = self._get_template(tpl_name)
        output: List[str] = []
        for raw in lines:
            keys = PLACEHOLDER_PATTERN.findall(raw)
            missing = [k for k in keys if k not in vars_map]
            if missing:
                print(f"*** Warning: Template {tpl_name} references unknown vars: {missing}. ")
                continue
                # raise KeyError(f"Template {tpl_name} references unknown vars: {missing}")

            stripped = raw.strip()
            # standalone placeholder may be string or list
            if len(keys) == 1 and stripped == f"{{{{{keys[0]}}}}}":
                val = vars_map[keys[0]]
                # normalize to list of lines or items
                if isinstance(val, list):
                    items = val
                else:
                    items = [line for line in str(val).splitlines()]
                if not items:
                    continue
                indent = raw[: len(raw) - len(raw.lstrip())]
                for item in items:
                    if item:
                        output.append(indent + item)
            else:
                # inline: only string values
                line = raw
                for k in keys:
                    replacement = str(vars_map[k])
                    line = line.replace(f"{{{{{k}}}}}", replacement)
                output.append(line)

        return output
    