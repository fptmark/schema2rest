#!/usr/bin/env python3
"""
Simple standalone script to read and process an mmd file line by line.
Usage: python convert/tests/test_decorator.py <mmd_file_path>
"""
import sys
from pathlib import Path
import os
sys.path.append(str(Path(__file__).parent.parent.parent))

from convert.decorators import Decorator

def process_mmd_file(file_path):

    decorator = Decorator()
    try:
        # Check if file exists
        current_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = Path(current_dir, file_path)
        if not file_path.exists():
            print(f"Error: File {file_path} does not exist.")
            return
            
        # Open and read the file line by line
        
        with open(file_path, 'r', encoding='utf-8') as file:
            for line_num, line in enumerate(file, 1):
                # Strip trailing newline but preserve other whitespace
                line = line.rstrip('\n')
                if decorator.has_decorator(line):
                    decorator.process_decorations(line, "User", "email", "string")
            entities, dictionaries = decorator.get_objects()
            print(f'entity size = {len(entities)}, dictionaries = {len(dictionaries)}')
                
        
    except Exception as e:
        print(f"Error processing file: {e}")

if __name__ == "__main__":
    # Check if file path is provided
    if len(sys.argv) < 2:
        print("Usage: python convert/tests/test_decorator.py <mmd_file_path>")
        sys.exit(1)
        
    # Get the file path from command line argument
    mmd_file_path = sys.argv[1]
    
    # Process the file
    process_mmd_file(mmd_file_path)