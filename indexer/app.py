import io
import json
import logging
import os
import pickle
import re
from typing import Any

import pdfminer.high_level
import requests
from slack_sdk.web import WebClient
from sentence_transformers import SentenceTransformer
from slack_sdk.oauth.installation_store.sqlalchemy import SQLAlchemyInstallationStore
from sqlalchemy import create_engine, Column, Integer, PickleType, String, Text, DateTime
from sqlalchemy.sql import func
from sqlalchemy.ext.declarative import as_declarative, declared_attr
from sqlalchemy.orm import sessionmaker

logger = logging.getLogger()
logger.setLevel(logging.INFO)

user = os.environ["POSTGRES_USER"]
password = os.environ["POSTGRES_PASSWORD"]
host = os.environ["POSTGRES_HOST"]
database = os.environ["POSTGRES_DB"]
port = os.environ["POSTGRES_PORT"]

REGEX_EXP = r"(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|\?)\s"

SQLALCHEMY_DATABASE_URL = f"postgresql://{user}:{password}@{host}:{port}/{database}"
engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
installation_store = SQLAlchemyInstallationStore(
    client_id=os.environ["SLACK_CLIENT_ID"],
    engine=engine
)

search_model = SentenceTransformer(os.environ["DATA_DIR"])


@as_declarative()
class Base:
    id: Any
    __name__: str
    # Generate __tablename__ automatically

    @declared_attr
    def __tablename__(cls) -> str:
        return cls.__name__.lower()


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



def _get_txt_document_text(url, team):
    bot = installation_store.find_bot(
        enterprise_id=None,
        team_id=team,
    )
    headers = {
        "Authorization": f"Bearer {bot.bot_token}",
        "Content-Type": "text/html"
    }
    text = requests.get(url, headers=headers).text
    return text


def _get_pdf_document_text(url, team):
    bot = installation_store.find_bot(
        enterprise_id=None,
        team_id=team,
    )
    headers = {
        "Authorization": f"Bearer {bot.bot_token}",
    }
    byte_str = requests.get(url, headers=headers).content
    pdf_memory_file = io.BytesIO()
    pdf_memory_file.write(byte_str)
    text = pdfminer.high_level.extract_text(pdf_memory_file)
    return text


def handler(event, context):
    for record in event['Records']:
        if isinstance(record["body"], str):
            payload = json.loads(record["body"])
        else:
            payload = record["body"]
        file_name = payload["file_name"]
        url = payload["url"]
        team = payload["team"]
        user = payload["user"]
        channel = payload["channel"]
        file_id = payload["file_id"]
        logger.info(f"Processing File: {file_name} from Team: {team}")
        filetype = payload["filetype"]
        mimetype = payload["mimetype"]
        converted_pdf = payload.get("converted_pdf", None)
        text = None
        if mimetype == "text/plain":
            text = _get_txt_document_text(url, team)
        elif mimetype == "application/pdf":
            text = _get_pdf_document_text(url, team)
        elif filetype == "docx" and converted_pdf is not None:
            text = _get_pdf_document_text(url, team)
        else:
            continue
        sentences = re.split(REGEX_EXP, text)
        doc_embeddings = search_model.encode(sentences)
        db = SessionLocal()
        try:
            doc = Document(**{
                "team": team,
                "name": file_name,
                "user": user,
                "url": url,
                "embeddings": pickle.dumps(doc_embeddings),
                "file_id": file_id, 
            })
            db.add(doc)
            db.commit()
            db.refresh(doc)
        except:
            db.rollback()
            raise
        finally:
            db.close()
        bot = installation_store.find_bot(
            enterprise_id=None,
            team_id=team,
        )
        client = WebClient(token=bot.bot_token)
        client.chat_postMessage(
            channel=channel,
            text=f"Finished processing File {file_name}"
        )