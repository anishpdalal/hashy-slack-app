import os
import uuid

from sqlalchemy import Column, Integer, PickleType, String, Text, DateTime
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.sql import func
from sqlalchemy_utils import EncryptedType
from sqlalchemy_utils.types.encrypted.encrypted_type import AesEngine


from .base_class import Base


class Query(Base):
    id = Column(Integer, primary_key=True, index=True)
    query_id = Column(String)
    team = Column(String, nullable=False)
    user = Column(String, nullable=False)
    channel = Column(String)
    time_created = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    time_updated = Column(DateTime(timezone=True), onupdate=func.now())
    upvotes = Column(Integer)
    voters = Column(ARRAY(String))


class Document(Base):
    id = Column(Integer, primary_key=True, index=True)
    team = Column(String, nullable=False)
    user = Column(String)
    users = Column(ARRAY(String))
    file_id = Column(String, nullable=False)
    name = Column(String, nullable=False)
    type = Column(String)
    url = Column(String, nullable=False)
    embeddings = Column(PickleType)
    num_vectors = Column(Integer)
    time_created = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    time_updated = Column(DateTime(timezone=True), onupdate=func.now())


class LoggedUser(Base):
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(String, nullable=False)
    team_name = Column(String)
    team_id = Column(String)
    time_created = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    time_updated = Column(DateTime(timezone=True), onupdate=func.now())


class NotionToken(Base):
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(String, nullable=False)
    team = Column(String, nullable=False)
    notion_user_id = Column(String, nullable=False)
    encrypted_token = Column(EncryptedType(String, os.environ["TOKEN_SEC_KEY"], AesEngine, "pkcs5"))
    time_created = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    bot_id = Column(String, nullable=False)
    workspace_id = Column(String, nullable=False)
    channel_id = Column(String)
    last_cursor = Column(String)
    time_updated = Column(DateTime(timezone=True), onupdate=func.now())


class GoogleToken(Base):
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(String, nullable=False)
    team = Column(String, nullable=False)
    encrypted_token = Column(EncryptedType(String, os.environ["TOKEN_SEC_KEY"], AesEngine, "pkcs5"))
    time_created = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    channel_id = Column(String)
 