import datetime
import itertools
import json
import logging
import os
from typing import Any

import boto3
import pytz
import requests
from sqlalchemy import create_engine, Column, Integer, PickleType, String, Text, DateTime
from sqlalchemy.sql import func
from sqlalchemy.ext.declarative import as_declarative, declared_attr
from sqlalchemy.orm import load_only, sessionmaker
from sqlalchemy_utils import EncryptedType
from sqlalchemy_utils.types.encrypted.encrypted_type import AesEngine


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
    encrypted_token = Column(EncryptedType(String, os.environ["TOKEN_SEC_KEY"], AesEngine, "pkcs5"))
    time_created = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    bot_id = Column(String, nullable=False)
    workspace_id = Column(String, nullable=False)
    channel_id = Column(String)


class Document(Base):
    id = Column(Integer, primary_key=True, index=True)
    team = Column(String, nullable=False)
    user = Column(String, nullable=False)
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



logger = logging.getLogger()
logger.setLevel(logging.INFO)

pg_user = os.environ["POSTGRES_USER"]
password = os.environ["POSTGRES_PASSWORD"]
host = os.environ["POSTGRES_HOST"]
database = os.environ["POSTGRES_DB"]
port = os.environ["POSTGRES_PORT"]


def chunks(iterable, batch_size=10):
    """A helper function to break an iterable into chunks of size batch_size."""
    it = iter(iterable)
    chunk = tuple(itertools.islice(it, batch_size))
    while chunk:
        yield chunk
        chunk = tuple(itertools.islice(it, batch_size))


def handler(event, context):
    SQLALCHEMY_DATABASE_URL = f"postgresql://{pg_user}:{password}@{host}:{port}/{database}"
    engine = create_engine(SQLALCHEMY_DATABASE_URL)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    db = SessionLocal()
    sqs = boto3.resource("sqs", region_name="us-east-1")
    queue = sqs.get_queue_by_name(QueueName=os.getenv("SQS_QUEUE_NAME"))
    try:
        team = event.get("team")
        user = event.get("user")
        if team and user:
            token = db.query(NotionToken).filter(
                NotionToken.user_id == user,
                NotionToken.team == team,
            ).first()
            tokens = [token] if token else []
        else:
            tokens = db.query(NotionToken).all()
        doc_ids = []
        returned_doc_ids = []
        upserts = []
        deletes = []
        for token in tokens:
            team_doc_ids = [
                doc.file_id for doc in db.query(Document).filter(
                    Document.team == token.team,
                    Document.user == token.user_id,
                    Document.type == "notion"
                ).options(load_only(Document.file_id, Document.url)).all()
            ]
            doc_ids.extend(team_doc_ids)
            search_results = []
            request_body = {
                "sort": {
                    "direction": "descending",
                    "timestamp": "last_edited_time"
                },
                "filter": {
                    "property": "object",
                    "value": "page"
                }
            }
            headers = {
                "Authorization": f"Bearer {token.encrypted_token}",
                "Content-type": "application/json",
                "Notion-Version": "2021-08-16"
            }
            api_url = "https://api.notion.com/v1/search"
            results = requests.post(api_url, headers=headers, data=json.dumps(request_body)).json()
            search_results.extend(results["results"])
            while results.get("has_more"):
                request_body["start_cursor"] = results["next_cursor"]
                results = requests.post(api_url, headers=headers, data=json.dumps(request_body)).json()
                search_results.extend(results["results"])
            for res in search_results:
                returned_doc_ids.append(res["id"])
                doc = db.query(Document).filter(Document.file_id == res["id"]).options(
                    load_only(Document.file_id, Document.time_created, Document.time_updated)).first()
                if doc:
                    last_updated_notion = datetime.datetime.strptime(res["last_edited_time"], "%Y-%m-%dT%H:%M:%S.%fZ")
                    last_updated_db = max(doc.time_created, doc.time_updated) if doc.time_updated else doc.time_created
                    last_updated_notion_aware = pytz.utc.localize(last_updated_notion)
                    if last_updated_db > last_updated_notion_aware:
                        continue
                url = res["url"]
                split_url = url.split("/")[-1].split("-")
                if len(split_url) == 1:
                    file_name = "Untitled"
                else:
                    file_name = " ".join(split_url[:-1])
                page = {
                    "team": token.team,
                    "user": token.user_id,
                    "url": url,
                    "filetype": "notion",
                    "file_name": file_name,
                    "file_id": res["id"]
                }
                upserts.append({"MessageBody": json.dumps(page), "Id": res["id"]})

        doc_ids = set(doc_ids)
        returned_doc_ids = set(returned_doc_ids)
        docs_to_delete = doc_ids - returned_doc_ids
        for doc_id in docs_to_delete:
            doc = db.query(Document).filter(Document.file_id == doc_id).first()
            page = {
                "team": doc.team,
                "user": doc.user,
                "file_id": doc_id,
                "num_vectors": doc.num_vectors,
                "type": "delete"
            }
            deletes.append({"MessageBody": json.dumps(page), "Id": doc_id})
    except Exception as e:
        logger.error(e)
        raise
    finally:
        db.close()
    
    engine.dispose()

    logger.info(f"Upserting {len(upserts)} docs")
    for chunk in chunks(upserts, batch_size=10):
        queue.send_messages(Entries=chunk)
    
    logger.info(f"Deleting {len(deletes)} docs")
    for chunk in chunks(deletes, batch_size=10):
        queue.send_messages(Entries=chunk)