"""
Decorator handling module for processing MMD decorators
"""
import re
from typing import Dict, List, Tuple, Set, Any, Optional
import json5

# Constants for decorators
DICTIONARY = "@dictionary"
UNIQUE = "@unique"
VALIDATE = "@validate"
INHERIT = "@inherit"
SERVICE = "@service"
UI = "@ui"

# All supported decorators
ALL_DECORATORS = [DICTIONARY, UNIQUE, VALIDATE, INHERIT, SERVICE, UI]
FIELD_DECORATORS = [UNIQUE, VALIDATE, UI]
ENTITY_DECORATORS = [INHERIT, SERVICE, UNIQUE]

class Decorator:
    """
    Class to handle all decorator processing
    """
    def __init__(self):
        # Track stats for each decorator type
        self.stats = {
            "validation": 0,
            "ui": 0,
            "dictionary": 0,
            "inherit": 0,
            "service": 0,
            "unique": 0,
        }
        
        # Store structured data
        self.entities = {}  # List of entity objects
        self.dictionaries = {}  # List of dictionary objects
        
        # Internal mapping for faster lookups
        # self._entity_map = {}  # entity_name -> entity object index
        # self._field_map = {}   # (entity_name, field_name) -> (entity_index, field_index)
        # self._dict_map = {}    # dict_name -> dict object index
    
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
        i1 = text.find("%%")
        i2 = text.find("@")
        if i2 > i1 and i1 >= 0:
            if i2 == i1 + 2: 
                words = text[i2+1:].split()
                return words[0] in ALL_DECORATORS
        
        # Check for decorator(s) where there are spaces between %% and @
        words = text.split()
        try:
            i1 = words.index("%%")
            return words[i1+1] in ALL_DECORATORS
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

        elif entity_name and field_name and field_type and decorator in FIELD_DECORATORS:
            self._process_field_entity_decorations(decorator, entity_name, field_name, field_type, decorator_text)

        elif entity_name and decorator in ENTITY_DECORATORS and field_name is None:
            self._process_entity_decorations(decorator, entity_name, decoration)
        
    
    def _process_field_entity_decorations(self, decorator: str, entity_name: str, field_name: str, field_type: str, text: str):
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
        
        if not self.has_decorator(text):
            return
            
        # Update entity based on decoration type
        if decorator == INHERIT:
            self._add_inherit(entity_name, text)
        elif decorator == SERVICE:
            self._add_service(entity_name, text)
        elif decorator == UNIQUE:
            self._add_unique(entity_name, text)


    # Handles @validate and @ui
    def _add_field_data(self, decorator, entity_name, field_name, field_type, text):
        # remove trailing comma for mutliple decorators on a line
        if text.endswith(','):
            text = text[:-1]

        data = json5.loads(text)
        if decorator == VALIDATE or self._validatate_ui_attributes(data):
            if isinstance(data, dict):
                entity = self.entities.setdefault(entity_name, {})
                fields = entity.setdefault('fields', {})
                field = fields.setdefault(field_name, {})
                field.setdefault(decorator[1:], {}).update(data)


    # Handles unique from a field or entity defn
    def _add_unique(self, entity_name, field_names):
        entity = self.entities.setdefault(entity_name, {})
        fields = [word.strip() for word in field_names.split('+')]
        entity.setdefault('unique', []).extend(fields)

    # Handles inherit from an entity
    def _add_inherit(self, entity_name, obj_name):
        entity = self.entities.setdefault(entity_name, {})
        entity.setdefault('inherits', []).extend(obj_name.strip())
    
    # Handles service from an entity
    def _add_service(self, entity_name, obj_name):
        entity = self.entities.setdefault(entity_name, {})
        entity.setdefault('services', []).extend(obj_name.strip())

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

        if isinstance(dict_content, dict):

        # Store in class variables
            self.dictionaries.setdefault(dict_name, {}).update(dict_content)

    def _get_entity(self, name):
        # if name in self.entities:
        #     return self.entities[name]
        # else:
            return self.entities.setdefault(name, {})
    

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
        return self.entities, self.dictionaries
                    
        