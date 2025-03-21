"""
Decorator handling module for processing MMD decorators
"""
import re
import sys
from typing import Dict, List, Tuple, Set, Any, Optional
import json5

# Constants for decorators
DICTIONARY = "@dictionary"
UNIQUE = "@unique"
VALIDATE = "@validate"
INHERIT = "@inherit"
SERVICE = "@service"
UI = "@ui"
OPERATION = "@operations"
OPERATIONS = ["create", "read", "update", "delete"]

# All supported decorators
FIELD_DECORATORS = [UNIQUE, VALIDATE, UI]
ENTITY_DECORATORS = [INHERIT, SERVICE, UNIQUE, OPERATION]
ALL_DECORATORS = FIELD_DECORATORS + ENTITY_DECORATORS #[DICTIONARY, UNIQUE, VALIDATE, INHERIT, SERVICE, UI]

class Decorator:
    """
    Class to handle all decorator processing
    """
    def __init__(self, entities):
        
        # Store structured data
        self.entities = entities
        self.dictionaries = {}
        
    
    def has_decorator(self, text: str) -> bool:
        """
        Check if a string contains a decorator
        
        Args:
            text: String to check
            
        Returns:
            True if the string contains a decorator
        """
        if not text or not text.strip():
            return False
            
        # Check for decorator(s) where there is no space between %% and @
        pos = text.find("%%@")
        if pos >= 0:
            words = text[pos+2:].split()
            return words[0] in ALL_DECORATORS

        # Check for decorator(s) where there are spaces between %% and @
        words = text.split()
        try:
            pos = words.index("%%")
            return words[pos+1] in ALL_DECORATORS
        except:
            return False
        

    def process_decorations(self, text: str, entity_name: Optional[str] = None, field_name: Optional[str] = None, field_type: Optional[str] = None):
        """
        Process decorations from text and store in class variables
        
        Args:
            text: Text containing decorators
            entity_name: Optional entity name
            field_name: Optional field name
        """
        text = text.strip()
        if not text or '@' not in text:
            return
            
        # Extract the decorator part
        index = text.find('@')
        if index == -1:
            return

        decorator_text = text[index:]   # extract decoration data
        decorator = decorator_text.split()[0] # get the decorator

        decoration = decorator_text[len(decorator):].strip() # get the decoration - remove the decorator
            
        # Process based on decorator type
        if decorator == DICTIONARY and text.startswith("%%"):
            self._process_dictionary(decoration)

        elif entity_name:
            if decorator in FIELD_DECORATORS and field_name:
                self._process_field_entity_decorations(decorator, entity_name, field_name, field_type, decorator_text)
            elif decorator in ENTITY_DECORATORS:
                self._process_entity_decorations(decorator, entity_name, decoration)

    
    def _process_field_entity_decorations(self, decorator: str, entity_name: str, field_name: Optional[str], field_type: Optional[str], text: str):
        # if there are multiple decorators, split and recurse
        index = text[1:].find('@')
        if index > -1:
            next_text = text[index:].strip()    # text for next decorator
            words = next_text.split(' ')        # get decorator name
            text = text[:index]                # text for current decorator
            self._process_field_entity_decorations(words[0], entity_name, field_name, field_type, next_text)
        if decorator in [VALIDATE, UI]:
            text = text[len(decorator):].strip()
            self._add_field_data(decorator, entity_name, field_name, field_type, text)
        elif decorator == UNIQUE:
            self._add_unique(entity_name, field_name)


    def _process_entity_decorations(self, decorator: str, entity_name: str, text: str):
        """
        Process entity-level decorations and store them in class variables
        
        Args:
            entity_name: Name of the entity
            text: Text containing decorators
            field_name: Optional field name
        """
        
        # Update entity based on decoration type
        if decorator == INHERIT or decorator == SERVICE:
            self._add_entity_decoration(decorator, entity_name, text)
        elif decorator == UNIQUE:
            self._add_unique(entity_name, text)
        elif decorator == OPERATION:
            operation = ''
            permissions = json5.loads(text)
            if isinstance(permissions, list):
                for elem in permissions:
                    if isinstance(elem, str) and elem.lower() in OPERATIONS:
                        operation = operation + elem[0].lower()
            if len(operation) > 0:
                self._add_entity_decoration(decorator, entity_name, operation)


    # Handles @validate and @ui
    def _add_field_data(self, decorator, entity_name, field_name, field_type, text):
        # remove trailing comma for mutliple decorators on a line
        if text.endswith(','):
            text = text[:-1]

        try:
            data = json5.loads(text)
        except:
            print(f'*** Error parsing line {text}')
            sys.exit(-1)
        if decorator == VALIDATE or self._validatate_ui_attributes(data):
            if isinstance(data, dict):
                entity = self.entities[entity_name]
                fields = entity['fields'] 
                field = fields[field_name]

                # UI Metadata goes in a subsection
                if decorator == UI:
                    field.setdefault('ui_metadata', {}).update(data)
                else:
                    field.update(data)


    # Handles unique from a field or entity defn
    def _add_unique(self, entity_name, field_names):
        entity = self.entities[entity_name] 
        fields = [word.strip() for word in field_names.split('+')]
        entity.setdefault('unique', []).append(fields)

    def _add_entity_decoration(self, decorator, entity_name, value):
        entity = self.entities[entity_name] 
        entity.setdefault(decorator[1:], []).append(value.strip())

    # Handles dictionary
    def _process_dictionary(self, text: str):
        """
        Process a dictionary decorator and store in class variables
        
        Args:
            text: Text containing the dictionary name followed by json definitions
        """
        # Extract dictionary name and content
        words = text.strip().split()
        dict_name = words[0]
        dictionary_text = ' '.join(words[1:])
        dict_content = json5.loads(dictionary_text)

        # Store in class variables
        if isinstance(dict_content, dict):
            self.dictionaries.setdefault(dict_name, {}).update(dict_content)


    def _validatate_ui_attributes(self, attributes) -> bool:
        """
        Process UI metadata and apply defaults where appropriate
        
        Args:
            attributes: UI attributes dictionary
        """
        # List of supported UI attributes
        supported_attrs = {
            "displayName": [],
            "display": ["always", "detail", "form", "hidden"],
            "widget": [
                "text", "textarea", "password", "email", "url", "number",
                "checkbox", "select", "multiselect", "date", "jsoneditor", "reference"
            ],
            "placeholder": [],
            "helpText": [],
            "readOnly": [],
            "displayAfterField": []
        }

        for key, value in attributes.items():
            if key not in supported_attrs:
                print(f'ui attribute {key} not supported')
                return False
            elif len(supported_attrs[key]) > 0 and value not in supported_attrs[key]:
                print(f'ui value {value} for attribute {key} not supported.  Allowed values are {supported_attrs[key]}')
                return False
        return True

    def get_objects(self):
        return self.dictionaries
                    
        