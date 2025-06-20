from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, List, Dict, Any
from datetime import datetime

class {{Entity}}Update(BaseModel):
  {{BaseFields}}

  {{Validators}}

  model_config = ConfigDict(from_attributes=True, validate_by_name=True)

