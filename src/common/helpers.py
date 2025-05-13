import os
from pathlib import Path
from itertools import takewhile
import re
from typing import List, Tuple, Dict

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

def write(path_root: str, backend: str, component_dir: str, file_name: str, lines: str | List[str], display: bool = True) -> None:
    if isinstance(lines, List):
        lines = "\n".join(lines)

    dir = Path(path_root) / backend / "app" 
    if len(component_dir) > 0: 
        dir = dir / component_dir 

    dir.mkdir(parents=True, exist_ok=True)
    
    with open(dir / file_name, "w") as f:
        f.write(lines)
    
    if display:
        print(f"Generated {dir}/{file_name}")