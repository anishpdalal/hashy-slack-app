from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class QueryBase(BaseModel):
    text: str
    result: str
    team: str
    user: str
    embedding: bytes
    evidence: Optional[str] = None


class QueryCreate(QueryBase):
    pass


class Query(QueryBase):
    id: int
    time_created: datetime
    time_updated: datetime

    class Config:
        orm_mode = True


class DocumentBase(BaseModel):
    team: str
    name: str
    word_positions: str
    embeddings: bytes
    url: str
    user: str
    file_id: str


class DocumentCreate(DocumentBase):
    pass


class Document(DocumentBase):
    id: int
    time_created: datetime
    time_updated: datetime

    class Config:
        orm_mode = True


class LoggedUserBase(BaseModel):
    user_id: str

class LoggedUserCreate(LoggedUserBase):
    pass


class LoggedUser(LoggedUserBase):
    id: int
    time_created: datetime
    time_updated: datetime