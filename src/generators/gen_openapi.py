#!/usr/bin/env python3
"""
Clean OpenAPI generator with consistent structure and no collapse issues.
"""

import importlib
import sys
import os
import json
from typing import Dict, Any, List, Optional
from pathlib import Path
from common.schema import Schema

class CleanOpenAPIGenerator:
    """Generate clean, consistent OpenAPI 3.0 specification"""
    
    def __init__(self):
        self.openapi_spec = {
            "openapi": "3.0.3",
            "info": {
                "title": "Events Management API",
                "description": "Clean, consistent API documentation",
                "version": "1.0.0"
            },
            "servers": [
                {
                    "url": "http://localhost:5500/api",
                    "description": "Development server"
                }
            ],
            "paths": {},
            "components": {
                "schemas": {}
            }
        }
        
    def generate(self, root_dir: str) -> Dict[str, Any]:
        """Generate complete OpenAPI specification"""
        print("🚀 Generating clean OpenAPI specification...")

        # Add the target project's root directory to Python path
        abs_root = os.path.abspath(root_dir)
        if abs_root not in sys.path:
            sys.path.insert(0, abs_root)

        schema = Schema(root_dir + "/schema.yaml")
        
        # Add root directory's parent to Python path for imports
        root_path = Path(root_dir).resolve()
        sys.path.insert(0, str(root_path.parent))

        # Generate schemas and paths for all entities
        for entity_name, model_class in schema.concrete_entities().items():
            print(f"   📄 Processing {entity_name}...")

            module_name = f"app.models.{entity_name.lower()}_model"
            try:
                module = importlib.import_module(module_name)
                model_class = getattr(module, entity_name)
            except ImportError as e:
                print(f"   ⚠️  Could not import {module_name}: {e}")
                continue

            entity_meta = model_class._metadata
            operations = entity_meta.get("operations", "crud")
            print(f"   🔧 {entity_name} operations: {operations}")
            
            self._generate_entity_schemas(entity_name, entity_meta)
            self._generate_entity_paths(entity_name, entity_meta)
        
        return self.openapi_spec
    
    def _generate_entity_schemas(self, entity_name: str, entity_meta: Dict[str, Any]):
        """Generate flat, simple schemas for an entity"""
        
        # Main entity schema - completely flat
        main_properties = {}
        main_required = []
        
        # Create schema - no auto-generated fields
        create_properties = {}
        create_required = []
        
        # Update schema - all optional
        update_properties = {}
        
        # Always add id field to main schema only (for GET responses)
        main_properties["id"] = {
            "type": "string",
            "example": "507f1f77bcf86cd799439011"
        }
        
        # Process each field into flat properties
        for field_name, field_meta in entity_meta.get("fields", {}).items():
            field_schema = self._convert_field_to_simple_schema(field_name, field_meta)
            
            # Add to main schema
            main_properties[field_name] = field_schema
            if field_meta.get("required"):
                main_required.append(field_name)
            
            # Add to create schema (skip auto-generated, auto-update, and id)
            if not field_meta.get("autoGenerate") and not field_meta.get("autoUpdate") and field_name != "id":
                create_properties[field_name] = field_schema.copy()
                if field_meta.get("required"):
                    create_required.append(field_name)
            
            # Add to update schema (skip auto-generated and auto-update)
            if not field_meta.get("autoGenerate") and not field_meta.get("autoUpdate"):
                update_properties[field_name] = field_schema.copy()
        
        # Store completely flat schemas
        self.openapi_spec["components"]["schemas"][entity_name] = {
            "type": "object",
            "properties": main_properties,
            "required": main_required
        }
        
        self.openapi_spec["components"]["schemas"][f"{entity_name}Create"] = {
            "type": "object",
            "properties": create_properties,
            "required": create_required
        }
        
        self.openapi_spec["components"]["schemas"][f"{entity_name}Update"] = {
            "type": "object",
            "properties": update_properties
        }
    
    def _convert_field_to_simple_schema(self, field_name: str, field_meta: Dict[str, Any]) -> Dict[str, Any]:
        """Convert field to simple, flat schema definition"""
        field_type = field_meta.get("type", "String")
        
        # Simple type mapping
        if field_type == "String":
            schema = {"type": "string"}
        elif field_type == "Integer":
            schema = {"type": "integer"}
        elif field_type == "Number":
            schema = {"type": "number"}
        elif field_type == "Boolean":
            schema = {"type": "boolean"}
        elif field_type == "Currency":
            schema = {"type": "number"}
        elif field_type in ["Date", "Datetime", "ISODate"]:
            schema = {"type": "string", "format": "date-time"}
        elif field_type == "ObjectId":
            schema = {"type": "string"}
        else:
            schema = {"type": "string"}
        
        # Add simple constraints
        if "min_length" in field_meta:
            schema["minLength"] = field_meta["min_length"]
        if "max_length" in field_meta:
            schema["maxLength"] = field_meta["max_length"]
        if "ge" in field_meta:
            schema["minimum"] = field_meta["ge"]
        if "le" in field_meta:
            schema["maximum"] = field_meta["le"]
        
        # Pattern validation
        if "pattern" in field_meta and "regex" in field_meta["pattern"]:
            schema["pattern"] = field_meta["pattern"]["regex"]
        
        # Enum values
        if "enum" in field_meta and "values" in field_meta["enum"]:
            schema["enum"] = field_meta["enum"]["values"]
        
        # Simple example
        schema["example"] = self._generate_simple_example(field_name, field_meta)
        
        return schema
    
    def _generate_simple_example(self, field_name: str, field_meta: Dict[str, Any]) -> Any:
        """Generate simple example values"""
        field_type = field_meta.get("type", "String")
        
        if field_type == "String":
            if "enum" in field_meta and "values" in field_meta["enum"]:
                return field_meta["enum"]["values"][0]
            
            # Simple field name patterns
            field_lower = field_name.lower()
            if "username" in field_lower:
                return "john_doe"
            elif "email" in field_lower:
                return "user@example.com"
            elif "password" in field_lower:
                return "securePassword123"
            elif "name" in field_lower:
                return "Example Name"
            else:
                return "example_value"
        
        elif field_type == "Integer":
            return 42
        elif field_type == "Number":
            return 123.45
        elif field_type == "Currency":
            return 1250.00
        elif field_type == "Boolean":
            return True
        elif field_type in ["Date", "Datetime", "ISODate"]:
            return "2024-07-15T14:30:00Z"
        elif field_type == "ObjectId":
            return "507f1f77bcf86cd799439011"
        
        return "example"
    
    def _generate_entity_paths(self, entity_name: str, entity_meta: Dict[str, Any]):
        """Generate consistent paths for an entity"""
        entity_lower = entity_name.lower()
        operations = entity_meta.get("operations", "crud")
        
        if not operations or operations.strip() == "":
            operations = "crud"
        
        # Collection endpoint
        collection_path = f"/{entity_lower}"
        self.openapi_spec["paths"][collection_path] = {}
        
        # GET collection (list)
        if "r" in operations:
            self.openapi_spec["paths"][collection_path]["get"] = {
                "summary": f"List {entity_name}s",
                "tags": [entity_name],
                "responses": {
                    "200": {
                        "description": "Success",
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "data": {
                                            "type": "array",
                                            "items": self.openapi_spec["components"]["schemas"][entity_name]
                                        }
                                    }
                                }
                            }
                        }
                    },
                    "500": self._error_response()
                }
            }
        
        # POST collection (create)
        if "c" in operations:
            self.openapi_spec["paths"][collection_path]["post"] = {
                "summary": f"Create {entity_name}",
                "tags": [entity_name],
                "requestBody": {
                    "required": True,
                    "content": {
                        "application/json": {
                            "schema": self.openapi_spec["components"]["schemas"][f"{entity_name}Create"]
                        }
                    }
                },
                "responses": {
                    "201": {
                        "description": "Created",
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "data": self.openapi_spec["components"]["schemas"][entity_name]
                                    }
                                }
                            }
                        }
                    },
                    "400": self._error_response(),
                    "409": self._error_response(),
                    "500": self._error_response()
                }
            }
        
        # Item endpoint
        item_path = f"/{entity_lower}/{{id}}"
        self.openapi_spec["paths"][item_path] = {}
        
        # GET item
        if "r" in operations:
            self.openapi_spec["paths"][item_path]["get"] = {
                "summary": f"Get {entity_name}",
                "tags": [entity_name],
                "parameters": [
                    {
                        "name": "id",
                        "in": "path",
                        "required": True,
                        "schema": {"type": "string"}
                    }
                ],
                "responses": {
                    "200": {
                        "description": "Success",
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "data": self.openapi_spec["components"]["schemas"][entity_name]
                                    }
                                }
                            }
                        }
                    },
                    "404": self._error_response(),
                    "500": self._error_response()
                }
            }
        
        # PUT item (update)
        if "u" in operations:
            self.openapi_spec["paths"][item_path]["put"] = {
                "summary": f"Update {entity_name}",
                "tags": [entity_name],
                "parameters": [
                    {
                        "name": "id",
                        "in": "path",
                        "required": True,
                        "schema": {"type": "string"}
                    }
                ],
                "requestBody": {
                    "required": True,
                    "content": {
                        "application/json": {
                            "schema": self.openapi_spec["components"]["schemas"][f"{entity_name}Update"]
                        }
                    }
                },
                "responses": {
                    "200": {
                        "description": "Updated",
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "data": self.openapi_spec["components"]["schemas"][entity_name]
                                    }
                                }
                            }
                        }
                    },
                    "400": self._error_response(),
                    "404": self._error_response(),
                    "409": self._error_response(),
                    "500": self._error_response()
                }
            }
        
        # DELETE item
        if "d" in operations:
            self.openapi_spec["paths"][item_path]["delete"] = {
                "summary": f"Delete {entity_name}",
                "tags": [entity_name],
                "parameters": [
                    {
                        "name": "id",
                        "in": "path",
                        "required": True,
                        "schema": {"type": "string"}
                    }
                ],
                "responses": {
                    "200": {
                        "description": "Deleted",
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "data": {"type": "boolean"}
                                    }
                                }
                            }
                        }
                    },
                    "404": self._error_response(),
                    "500": self._error_response()
                }
            }
    
    def _error_response(self):
        """Standard error response - completely inline"""
        return {
            "description": "Error",
            "content": {
                "application/json": {
                    "schema": {
                        "type": "object",
                        "properties": {
                            "data": {"nullable": True},
                            "message": {"type": "string"},
                            "level": {"type": "string"}
                        }
                    }
                }
            }
        }


def main():
    """Generate clean OpenAPI specification"""
    try:
        print("🚀 Clean OpenAPI Generator")
        
        dir = sys.argv[1] if len(sys.argv) > 1 else "."
        
        generator = CleanOpenAPIGenerator()
        spec = generator.generate(dir)
        
        # Save to file
        output_file = Path(dir, "openapi.json")
        with open(output_file, 'w') as f:
            json.dump(spec, f, indent=2)
        
        print(f"\n✅ Clean OpenAPI specification generated!")
        print(f"📄 Saved to: {output_file.absolute()}")
        print(f"📊 Generated {len(spec['components']['schemas'])} schemas")
        print(f"🛣️  Generated {len(spec['paths'])} API paths")
        
        entities = [name for name in spec['components']['schemas'].keys() 
                   if not name.endswith(('Create', 'Update'))]
        print(f"🏗️  Entities: {', '.join(entities)}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()