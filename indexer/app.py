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
from sqlalchemy_utils import EncryptedType
from sqlalchemy_utils.types.encrypted.encrypted_type import AesEngine


logger = logging.getLogger()
logger.setLevel(logging.INFO)

pg_user = os.environ["POSTGRES_USER"]
password = os.environ["POSTGRES_PASSWORD"]
host = os.environ["POSTGRES_HOST"]
database = os.environ["POSTGRES_DB"]
port = os.environ["POSTGRES_PORT"]

REGEX_EXP = r"(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|\?)\s"


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


def _get_txt_document_text(token, url):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "text/html"
    }
    text = requests.get(url, headers=headers).text
    return text


def _get_pdf_document_text(token, url):
    headers = {
        "Authorization": f"Bearer {token}",
    }
    byte_str = requests.get(url, headers=headers).content
    pdf_memory_file = io.BytesIO()
    pdf_memory_file.write(byte_str)
    text = pdfminer.high_level.extract_text(pdf_memory_file)
    return text


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


def _get_notion_document_text(file_id, token):
    try:
        api_url = f"https://api.notion.com/v1/blocks/{file_id}/children"
        headers = {
            "Authorization": f"Bearer {token}",
            "Notion-Version": "2021-08-16"
        }
        params = {}
        child_blocks = []
        results = requests.get(api_url, headers=headers).json()
        child_blocks.extend(results["results"])
        while results.get("has_more"):
            params["start_cursor"] = results["next_cursor"]
            results = requests.get(api_url, params=params).json()
            child_blocks.extend(results["results"])
        text = []
        todos = []
        for block in child_blocks:
            if block["type"] == "paragraph":
                for snippet in block["paragraph"]["text"]:
                    text.append(snippet["text"]["content"])
            elif block["type"] == "callout":
                for snippet in block["callout"]["text"]:
                    text.append(snippet["text"]["content"])
            elif block["type"] == "to_do":
                for snippet in block["to_do"]["text"]:
                    todos.append(snippet["text"]["content"])
            elif block["type"] == "bulleted_list_item":
                for snippet in block["bulleted_list_item"]["text"]:
                    todos.append(snippet["text"]["content"])
            elif block["type"] == "numbered_list_item":
                for snippet in block["numbered_list_item"]["text"]:
                    todos.append(snippet["text"]["content"])
            else:
                pass
        todos_text = ". ".join(todos)
        text.append(todos_text)
        processed_text = " ".join(" ".join(text).encode("ascii", "ignore").decode().strip().split())
    except Exception as e:
        logger.info(e)
        return None
    return processed_text


def handler(event, context):
    SQLALCHEMY_DATABASE_URL = f"postgresql://{pg_user}:{password}@{host}:{port}/{database}"
    engine = create_engine(SQLALCHEMY_DATABASE_URL)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    installation_store = SQLAlchemyInstallationStore(
        client_id=os.environ["SLACK_CLIENT_ID"],
        engine=engine
    )
    search_model = SentenceTransformer(os.environ["DATA_DIR"])
    logger.info(f"Processing {len(event['Records'])}")
    for record in event['Records']:
        if isinstance(record["body"], str):
            payload = json.loads(record["body"])
        else:
            payload = record["body"]
        logger.info(record['body'])
        file_name = payload["file_name"]
        url = payload["url"]
        team = payload["team"]
        user = payload["user"]
        channel = payload.get("channel")
        file_id = payload["file_id"]
        filetype = payload["filetype"]
        mimetype = payload.get("mimetype")
        converted_pdf = payload.get("converted_pdf", None)
        text = None
        bot = installation_store.find_bot(
            enterprise_id=None,
            team_id=team,
        )
        token = bot.bot_token
        db = SessionLocal()
        try:
            token = db.query(NotionToken).filter(NotionToken.user_id == user).first().encrypted_token
        except:
            raise
        finally:
            db.close()
        if mimetype == "text/plain":
            text = _get_txt_document_text(token, url)
        elif mimetype == "application/pdf":
            text = _get_pdf_document_text(token, url)
        elif filetype == "docx" and converted_pdf is not None:
            text = _get_pdf_document_text(token, url)
        elif filetype == "notion":
            text = _get_notion_document_text(file_id, token)
            if not text:
                continue
        else:
            continue
        sentences = re.split(REGEX_EXP, text)
        doc_embeddings = search_model.encode(sentences)
        db = SessionLocal()
        fields = {
            "team": team,
            "name": file_name,
            "user": user,
            "url": url,
            "embeddings": pickle.dumps(doc_embeddings),
            "file_id": file_id, 
        }
        try:
            doc = db.query(Document).filter(Document.file_id == file_id).first()
            if doc:
                db.query(Document).filter_by(id=doc.id).update(fields)
                db.commit()
            else:
                doc = Document(**fields)
                db.add(doc)
                db.commit()
                db.refresh(doc)
        except:
            db.rollback()
            raise
        finally:
            db.close()
                
        if channel:
            bot = installation_store.find_bot(
                enterprise_id=None,
                team_id=team,
            )
            client = WebClient(token=bot.bot_token)
            client.chat_postMessage(
                channel=channel,
                text=f"Finished processing File {file_name}"
            )
    
    engine.dispose()