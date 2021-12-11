import datetime
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
    access_token = Column(String, nullable=False)
    time_created = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    bot_id = Column(String, nullable=False)
    workspace_id = Column(String, nullable=False)


class Document(Base):
    id = Column(Integer, primary_key=True, index=True)
    team = Column(String, nullable=False)
    user = Column(String, nullable=False)
    file_id = Column(String, nullable=False)
    name = Column(String, nullable=False)
    word_positions = Column(Text)
    url = Column(String, nullable=False)
    embeddings = Column(PickleType, nullable=False)
    time_created = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    time_updated = Column(DateTime(timezone=True), onupdate=func.now())



logger = logging.getLogger()
logger.setLevel(logging.INFO)

user = os.environ["POSTGRES_USER"]
password = os.environ["POSTGRES_PASSWORD"]
host = os.environ["POSTGRES_HOST"]
database = os.environ["POSTGRES_DB"]
port = os.environ["POSTGRES_PORT"]

NOTION_REQUEST_BODY = {
    "sort": {
        "direction": "descending",
        "timestamp": "last_edited_time"
    },
    "filter": {
        "property": "object",
        "value": "page"
    }
}


def handler(event, context):
    SQLALCHEMY_DATABASE_URL = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(SQLALCHEMY_DATABASE_URL)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    db = SessionLocal()
    sqs = boto3.resource("sqs", region_name="us-east-1")
    queue = sqs.get_queue_by_name(QueueName=os.getenv("SQS_QUEUE_NAME"))
    try:
        tokens = db.query(NotionToken).all()
        for token in tokens:
            search_results = requests.post(
                "https://api.notion.com/v1/search",
                headers={
                    "Authorization": f"Bearer {token.access_token}",
                    "Content-type": "application/json",
                    "Notion-Version": "2021-08-16"
                },
                data=json.dumps(NOTION_REQUEST_BODY)
            ).json()["results"]
            for res in search_results:
                doc = db.query(Document).filter(Document.file_id == res["id"]).options(
                    load_only(Document.file_id, Document.time_created, Document.time_updated)).first()
                if doc:
                    last_updated_notion = datetime.datetime.strptime(res["last_edited_time"], "%Y-%m-%dT%H:%M:%S.%fZ")
                    last_updated_db = max(doc.time_created, doc.time_updated)
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
                logger.info(page)
                queue.send_message(MessageBody=json.dumps(page))

    except:
        raise
    finally:
        db.close()
    