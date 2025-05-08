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
SELECTOR = "@selector"

OPERATIONS = ["create", "read", "update", "delete"]

# All supported decorators
COMMON_DECORATORS = [UI, UNIQUE]
FIELD_DECORATORS = COMMON_DECORATORS + [VALIDATE]
ENTITY_DECORATORS = COMMON_DECORATORS + [SERVICE, OPERATION, ABSTRACT, INCLUDES, SELECTOR]
ALL_DECORATORS = FIELD_DECORATORS + ENTITY_DECORATORS + [DICTIONARY]


# Constants 
FIELDS = 'fields'
UI_METADATA = 'ui'
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
            if decorator in COMMON_DECORATORS and not field_name:  # check if we have an field decorator
                decoration, field_name = self._get_field_name(decoration, field_name)
            if field_name and decorator in FIELD_DECORATORS:
                self._process_field_entity_decorations(decorator, entity_name, field_name, decoration)
            # Handle UI and UNIQUE decorators at the entity level
            elif decorator in ENTITY_DECORATORS:
                # Handle UI and UNIQUE at the entity level
                self._process_entity_decorations(decorator, entity_name, decoration)


    def _process_field_entity_decorations(self, decorator: str, entity_name: str, field_name: Optional[str], text: str):
        # if there are multiple decorators, split and recurse
        index = text.find('@')
        if index > -1:
            # process current decorator and then recurse
            current_text = text[:index].strip()         # text for current decorator
            self._process_field_entity_decorations(decorator, entity_name, field_name, current_text)
            
            # process next decorator
            text = text[index:].strip()         # text for next decorator
            decorator = text.split(' ')[0]      # get decorator name
            text = text[len(decorator):].strip()     # remove the decorator from the text
            self._process_field_entity_decorations(decorator, entity_name, field_name, text)
        elif decorator in [VALIDATE, UI]:
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
        if decorator == UI:       # may be an entity or field decorator
            words = text.split(' ')
            if words[0] != '{':     # assume the word is an field name so process as a field decorator
                self._add_field_data(decorator, entity_name, words[0], ' '.join(words[1:]))
                return

        if decorator in [ ABSTRACT, INCLUDES, SERVICE, UI, SELECTOR]:
            self._add_entity_decoration(decorator, entity_name, text)
        elif decorator == UNIQUE:
            self._add_unique(entity_name, text)

        elif decorator == OPERATION:
            operation: str = ''
            permissions = json5.loads(text)
            if isinstance(permissions, list):
                for elem in permissions:
                    if isinstance(elem, str) and elem.lower() in OPERATIONS:
                        operation = operation + elem[0].lower()
            if len(operation) > 0:
                self._add_entity_decoration(decorator, entity_name, operation)


    # def _get_fields(self, entity_name: str) -> List[str]:
    #     entity = self.entities.get(entity_name)
    #     fields = []
    #     for field, _ in entity.get(FIELDS, {}).items():
    #         fields.append(field)
    #     return fields


    def _get_field_name(self, decoration, field_name):
        words = decoration.split(' ')
        if words[0] != '{':
            field_name = words[0]
            decoration = ' '.join(words[1:]).strip()  # remove the decorator from the text
        return decoration, field_name


    # Handles @validate and @ui
    # Note: Selector is different.  It is processed like an entity decorator but in fact it is a field decorations
    # This is due to the fact that it affects a generated field (foreign key) due to a defined relationship
    def _add_field_data(self, decorator, entity_name, field_name, value):
        if decorator == SELECTOR:
            field_name  = field_name.lower() + "Id"

            entity = self.entities[entity_name]
            fields = entity[FIELDS] 
            field =fields.setdefault(field_name, {})
            field.setdefault( SELECTOR[1:], value )

        else:
            field_name = "_id" if field_name.lower() == "id" else field_name
            # remove trailing comma for mutliple decorators on a line
            if value.endswith(','):
                value = value[:-1]

            try:
                data = json5.loads(value)
            except:
                print(f'*** Error parsing line {value}')
                sys.exit(-1)
            if decorator == VALIDATE or self._validatate_ui_attributes(data):
                if isinstance(data, dict):
                    # Handle dictioary lookup - do it in gen_models instead
                    # if 'pattern' in data and 'regex' in data['pattern']:
                    #     regex = data['pattern']['regex']
                    #     if regex.startswith("dictionary="):
                    #         words = regex.split('=')[1].split('.')
                    #         data['pattern']['regex'] = self.dictionaries[words[0]][words[1]]

                    entity = self.entities[entity_name]
                    fields = entity[FIELDS] 
                    field =fields.setdefault(field_name, {})

                    # UI Metadata goes in a subsection
                    if decorator == UI:
                        field.setdefault(UI_METADATA, {}).update(data)
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
                    field_value.setdefault(UI_METADATA, {}).update({'displayAfterField': str(prior_field)})
                    prior_field = prior_field - 1

                entity.setdefault(FIELDS, []).update(fields_copy)
            else:
                print(f'*** Error: abstraction fields for {value} not found')
                sys.exit(-1)
        elif decorator == SELECTOR:
            data = json5.loads(value)
            if data and isinstance(data, dict):
                key, value = next(iter(data.items()))
                self._add_field_data(decorator, entity_name, key, value)
        elif decorator == UI or decorator == SELECTOR:
            data = json5.loads(value)
            entity.setdefault(UI_METADATA, {}).update(data)
        elif decorator == OPERATION:
            entity.setdefault(decorator[1:], value.strip())
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
            "displayAfterField": [],
            "displayPages": [],
            "clientEdit": []
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
                    
        