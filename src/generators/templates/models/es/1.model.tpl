from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, ConfigDict, field_validator
from beanie import PydanticObjectId
import re
from app.db import get_es_client

class {{Entity}}BaseModel):
    __index__ = "{{EntityLower}}"
    __unique__ = {{UniqueList}}
    __mappings__ = {{MappingsDict}}

    model_config = ConfigDict(populate_by_name=True)

    {{BaseFields}}

    {{AutoFields}}

    {{SaveFunction}}
