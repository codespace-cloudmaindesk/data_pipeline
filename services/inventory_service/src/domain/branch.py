from pydantic import BaseModel
from uuid import UUID
from typing import Optional

class Branch(BaseModel):
    id: UUID
    name: str
    location: Optional[str]

    class Config:
        orm_mode = True
