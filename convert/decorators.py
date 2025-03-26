"""
Decorator handling module for processing MMD decorators
"""
import copy
import re
import sys
from typing import Dict, List, Tuple, Set, Any, Optional
import json5

# Constants for decorators
DICTIONARY = "@dictionary"
UNIQUE = "@unique"
VALIDATE = "@validate"
ABSTRACT = "@abstract"
INCLUDES = "@include"
SERVICE = "@service"
UI = "@ui"
OPERATION = "@operations"
OPERATIONS = ["create", "read", "update", "delete"]

# All supported decorators
FIELD_DECORATORS = [UNIQUE, VALIDATE, UI]
ENTITY_DECORATORS = [SERVICE, UNIQUE, OPERATION, UI, ABSTRACT, INCLUDES]
ALL_DECORATORS = FIELD_DECORATORS + ENTITY_DECORATORS + [DICTIONARY]


# Constants 
FIELDS = 'fields'
METADATA = 'ui_metadata'
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
            decorator_text = decorator_text[len(decorator):].strip()  # remove the decorator from the text
            if decorator in FIELD_DECORATORS and field_name:
                self._process_field_entity_decorations(decorator, entity_name, field_name, decorator_text)
            elif decorator in ENTITY_DECORATORS:
                self._process_entity_decorations(decorator, entity_name, decoration)

    
    def _process_field_entity_decorations(self, decorator: str, entity_name: str, field_name: Optional[str], text: str):
        # if there are multiple decorators, split and recurse
        index = text.find('@')
        if index > -1:
            next_text = text[index:].strip()    # text for next decorator
            words = next_text.split(' ')        # get decorator name
            text = text[:index].strip()         # text for current decorator
            next_text = next_text[len(words[0]):].strip()  # remove the decorator from the text
            self._process_field_entity_decorations(words[0], entity_name, field_name, next_text)
        if decorator in [VALIDATE, UI]:
            # text = text[len(decorator):].strip()
            self._add_field_data(decorator, entity_name, field_name, text)
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
        if decorator == ABSTRACT or decorator == INCLUDES or decorator == SERVICE:
            self._add_entity_decoration(decorator, entity_name, text)
        elif decorator == UNIQUE:
            self._add_unique(entity_name, text)
        elif decorator == UI:
            # handle all 5 forms of UI decorator at entity level - <entity>.*, <entity>.<field>, id, <field>, *
            words = text.split(' ')
            if words[0] == '{' or words[1] != '{': # This should always be false
                print(f'*** Error: UI entity decorator for {entity_name} must include fields: {text}')
                sys.exit(-1)
            entity_defn = words[0].split('.')
            # Process id field
            if words[0] == 'id':
                field = words[0]
                fields = [field]
            # check for named/inherited entity - if not it is the current entity
            elif len(entity_defn) > 1:
                fields = self._get_fields(entity_defn[0])
                field = entity_defn[1]
            else:
                fields = self._get_fields(entity_name)  # use all fields for the current entity
                field = words[0]

            if field != '*' and field in fields:
                fields = [field]

            text = text[text.index('{'):]
            for field in fields:
                self._process_field_entity_decorations(UI, entity_name, field, text)

        elif decorator == OPERATION:
            operation = ''
            permissions = json5.loads(text)
            if isinstance(permissions, list):
                for elem in permissions:
                    if isinstance(elem, str) and elem.lower() in OPERATIONS:
                        operation = operation + elem[0].lower()
            if len(operation) > 0:
                self._add_entity_decoration(decorator, entity_name, operation)


    def _get_fields(self, entity_name: str) -> List[str]:
        entity = self.entities.get(entity_name)
        fields = []
        for field, _ in entity.get(FIELDS, {}).items():
            fields.append(field)
        return fields


    # Handles @validate and @ui
    def _add_field_data(self, decorator, entity_name, field_name, text):
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
                fields = entity[FIELDS] 
                field =fields.setdefault(field_name, {})

                # UI Metadata goes in a subsection
                if decorator == UI:
                    field.setdefault(METADATA, {}).update(data)
                else:
                    field.update(data)


    # Handles unique from a field or entity defn
    def _add_unique(self, entity_name, field_names):
        entity = self.entities[entity_name] 
        fields = [word.strip() for word in field_names.split('+')]
        entity.setdefault('unique', []).append(fields)

    def _add_entity_decoration(self, decorator, entity_name, value):
        entity = self.entities[entity_name] 
        if decorator == ABSTRACT:
            entity['abstract'] = True
        elif decorator == INCLUDES:
            # add a copy of the abstraction fields to the current entity.  set the displayAfterField so they all appear after the core entity fields
            abstraction = self.entities.get(value)
            if abstraction and FIELDS in abstraction:
                fields_copy = copy.deepcopy(abstraction[FIELDS])

                # set the display order - the included entity is after the core entity fields
                # entity_names = list(entity[FIELDS].keys())
                # prior_field = entity_names[-1]
                prior_field = -1    # special naming for included entity fields
                for field_name, field_value in fields_copy.items():
                    field_value.setdefault(METADATA, {}).update({'displayAfterField': str(prior_field)})
                    prior_field = prior_field - 1

                entity.setdefault(FIELDS, []).update(fields_copy)
            else:
                print(f'*** Error: abstraction fields for {value} not found')
                sys.exit(-1)
        else:
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
            "display": [],
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
                    
        