#!/usr/bin/env python3
import re
from typing import Dict, List, Mapping, Union
from pathlib import Path

PLACEHOLDER_PATTERN = re.compile(r"\{\{(\w+)\}\}")

class Templates:

    templates: dict[str, list[str]] = {}

    # base_dir is the path where the template directory exists
    def __init__(self, base_dir: Path, component: str, backend: str):
        template_dir = Path(base_dir) / "templates" / component / backend
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
    
    def render(self, tpl_name: str, vars_map: Mapping[str, Union[str, List[str]]]):
        """
        Single‐pass {Key}→vars_map[Key] substitution.
        - Error if tpl_name references a var not in vars_map.
        - If a line is exactly "{Key}" and vars_map[Key]=="" or [], skip that line.
        - If a line is exactly "{Key}" and vars_map[Key] is multiline (str with \n or List[str]),
        split on \n or on list items and prefix each with the same indentation,
        preserving any leading spaces in the var text.
        """
        lines = self._get_template(tpl_name)
        out: List[str] = []
        for raw in lines:
            keys = PLACEHOLDER_PATTERN.findall(raw)
            # ensure all placeholders exist
            missing = [k for k in keys if k not in vars_map]
            if missing:
                raise KeyError(f"Template {tpl_name} references unknown vars: {missing}")

            stripped = raw.strip()
            # standalone placeholder?
            if len(keys) == 1 and stripped == f"{{{keys[0]}}}":
                val = vars_map[keys[0]]
                # normalize to list of lines
                if isinstance(val, list):
                    lines_to_emit = val
                else:
                    lines_to_emit = val.splitlines()

                if not lines_to_emit:
                    continue

                indent = raw[: len(raw) - len(raw.lstrip())]
                for vline in lines_to_emit:
                    # skip truly empty strings
                    if vline != "":
                        out.append(indent + vline)
                continue

            # inline replacement
            line = raw
            for k in keys:
                v = vars_map[k]
                # if list, join into one string
                if isinstance(v, list):
                    v = "\n".join(v)
                line = line.replace(f"{{{{{k}}}}}", v)
            out.append(line)

        return out


    # def render(self, tpl_name: str, vars_map: Dict[str, str]):
    #     """
    #     Single‐pass {Key}→vars_map[Key] substitution.
    #     - Error if tpl_name references a var not in vars_map.
    #     - If a line is exactly "{Key}" and vars_map[Key]=="" skip that line.
    #     - If a line is exactly "{Key}" and vars_map[Key] is multiline,
    #       split on \n and prefix each with the same indentation,
    #       preserving any leading spaces in the var text.
    #     """
    #     lines = self._get_template(tpl_name)
    #     out: List[str] = []
    #     for raw in lines:
    #         keys = PLACEHOLDER_PATTERN.findall(raw)
    #         missing = [k for k in keys if k not in vars_map]
    #         if missing:
    #             continue
    
    #         stripped = raw.strip()
    #         # standalone placeholder?
    #         if len(keys) == 1 and stripped == f"{{{keys[0]}}}":
    #             val = vars_map[keys[0]]
    #             if val == "":
    #                 continue
    #             indent = raw[: len(raw) - len(raw.lstrip()) ]
    #             for vline in val.splitlines():
    #                 # only skip truly empty lines, keep lines that are spaces
    #                 if vline != "":
    #                     out.append(indent + vline)
    #             continue
    
    #         # inline replacement
    #         line = raw
    #         for k in keys:
    #             line = line.replace(f"{{{k}}}", vars_map[k])
    #         out.append(line)
    
    #     return out
    
