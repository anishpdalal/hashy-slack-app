import logging
import os
from typing import Any

from sqlalchemy import Column, Integer, PickleType, String, DateTime
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import load_only
from sqlalchemy.sql import func
from sqlalchemy.ext.declarative import as_declarative, declared_attr
from sqlalchemy_utils import EncryptedType
from sqlalchemy_utils.types.encrypted.encrypted_type import AesEngine

logger = logging.getLogger()
logger.setLevel(logging.INFO)


@as_declarative()
class Base:
    id: Any
    __name__: str
    # Generate __tablename__ automatically

    @declared_attr
    def __tablename__(cls) -> str:
        return cls.__name__.lower()


class NotionToken(Base):
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(String, nullable=False)
    team = Column(String, nullable=False)
    notion_user_id = Column(String, nullable=False)
    encrypted_token = Column(
        EncryptedType(
            String,
            os.environ["TOKEN_SEC_KEY"],
            AesEngine,
            "pkcs5"
        )
    )
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
    encrypted_token = Column(
        EncryptedType(
            String,
            os.environ["TOKEN_SEC_KEY"],
            AesEngine,
            "pkcs5"
        )
    )
    time_created = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    channel_id = Column(String)
    time_updated = Column(DateTime(timezone=True), onupdate=func.now())
    last_cursor = Column(String)


class Document(Base):
    id = Column(Integer, primary_key=True, index=True)
    team = Column(String, nullable=False)
    user = Column(String, nullable=False)
    users = Column(ARRAY(String))
    file_id = Column(String, nullable=False)
    name = Column(String, nullable=False)
    url = Column(String, nullable=False)
    type = Column(String)
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


def get_unique_teams(db):
    return [
        user.team_id for user in db.query(LoggedUser).distinct(
            LoggedUser.team_id
        ).options(load_only(LoggedUser.team_id)).all()
    ]


def get_notion_tokens(db):
    return db.query(NotionToken).order_by(NotionToken.time_updated.asc()).all()


def get_google_tokens(db):
    return db.query(GoogleToken).order_by(GoogleToken.time_updated.asc()).all()


def get_notion_token(db, user_id, team_id):
    token = db.query(NotionToken).filter(
        NotionToken.user_id == user_id,
        NotionToken.team == team_id,
    ).first()
    return token


def get_google_token(db, user_id, team_id):
    token = db.query(GoogleToken).filter(
        GoogleToken.user_id == user_id,
        GoogleToken.team == team_id,
    ).first()
    return token


def update_last_cursor_notion(db, id, last_cursor):
    try:
        db.query(NotionToken).filter_by(id=id).update({
            "last_cursor": last_cursor
        })
        db.commit()
    except Exception as e:
        logger.error(e)


def update_last_cursor_google(db, id, last_cursor):
    try:
        db.query(GoogleToken).filter_by(id=id).update({
            "last_cursor": last_cursor
        })
        db.commit()
    except Exception as e:
        logger.error(e)


def get_document(db, file_id):
    doc = db.query(Document).filter(Document.file_id == file_id).first()
    return doc


def get_user_notion_documents(db, team_id, user_id):
    docs = db.query(Document).filter(
        Document.team == team_id,
        Document.type == "notion",
        Document.users.any(user_id)
    ).options(load_only(Document.file_id)).all()
    return [doc.id for doc in docs]
