from datetime import datetime
from typing import List, Optional

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
    team_id: Optional[str]
    team_name: Optional[str]


class LoggedUserCreate(LoggedUserBase):
    pass


class LoggedUser(LoggedUserBase):
    id: int
    time_created: datetime
    time_updated: datetime


class NotionTokenBase(BaseModel):
    user_id: str
    team: str
    notion_user_id: str
    encrypted_token: str
    bot_id: str
    workspace_id: str


class NotionTokenCreate(NotionTokenBase):
    pass


class NotionToken(NotionTokenBase):
    time_created: datetime


class GooglePickerUpload(BaseModel):
    file_ids: List[str]
    team: str
    user: str
    token: str
    channel: str