from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, List, Dict, Any
from datetime import datetime

class {{Entity}}Read(BaseModel):
  id: str = Field(alias="_id")
  {{BaseFields}}
  {{AutoFields}}

  model_config = ConfigDict(from_attributes=True, populate_by_name=True)


