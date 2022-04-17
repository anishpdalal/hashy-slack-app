import os

from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.sql import func
from sqlalchemy_utils import EncryptedType
from sqlalchemy_utils.types.encrypted.encrypted_type import AesEngine


from .base_class import Base


class SlackUser(Base):
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(String, nullable=False)
    team_name = Column(String, nullable=False)
    team_id = Column(String, nullable=False)
    created = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated = Column(DateTime(timezone=True), onupdate=func.now())


class ContentStore(Base):
    id = Column(Integer, primary_key=True, index=True)
    team_id = Column(String, nullable=False)
    type = Column(String, nullable=False)
    source_id = Column(String, nullable=False, unique=True)
    name = Column(String)
    user_ids = Column(ARRAY(String))
    source_last_updated = Column(DateTime(timezone=True))
    url = Column(String)
    num_vectors = Column(Integer)
    created = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated = Column(DateTime(timezone=True), onupdate=func.now())
    engagement = Column(ARRAY(String))


class Integration(Base):
    id = Column(Integer, primary_key=True, index=True)
    team_id = Column(String, nullable=False)
    type = Column(String, nullable=False)
    token = Column(
        EncryptedType(String, os.environ["TOKEN_SEC_KEY"],
        AesEngine,
        "pkcs5"
    ), nullable=False)
    user_id = Column(String)
    created = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated = Column(DateTime(timezone=True), onupdate=func.now())
    last_cursor = Column(String)
