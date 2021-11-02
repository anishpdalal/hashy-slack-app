from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class QueryBase(BaseModel):
    text: str


class QueryCreate(QueryBase):
    pass


class Query(QueryBase):
    id: int
    result: str
    evidence: Optional[str] = None
    time_created: datetime
    time_updated: datetime
