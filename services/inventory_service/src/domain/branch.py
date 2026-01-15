from pydantic import BaseModel
from uuid import UUID
from typing import Optional
from .enums import branch_code

class Branch(BaseModel):
    id: UUID
    branch_code: branch_code
    location: Optional[str]

    model_config = {
        "from_attributes": True
    }
