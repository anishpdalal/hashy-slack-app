import io
import json
import logging
import os
import pickle
import re
from typing import Any

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
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
logging.getLogger("pdfminer").setLevel(logging.WARNING)
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
    type = Column(String)
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


class GoogleToken(Base):
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(String, nullable=False)
    team = Column(String, nullable=False)
    encrypted_token = Column(EncryptedType(String, os.environ["TOKEN_SEC_KEY"], AesEngine, "pkcs5"))
    time_created = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


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


def _get_gdrive_pdf_text(file_id, token):
    creds = Credentials.from_authorized_user_info({
        "refresh_token": token.encrypted_token,
        "client_id": os.environ["CLIENT_ID"],
        "client_secret": os.environ["CLIENT_SECRET"],
        "scopes": ["https://www.googleapis.com/auth/drive.file"]
    })
    creds.refresh(Request())
    service = build("drive", "v3", credentials=creds)
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
    text = pdfminer.high_level.extract_text(fh)
    return text


def _read_paragraph_element(element):
    """Returns the text in the given ParagraphElement.

        Args:
            element: a ParagraphElement from a Google Doc.
    """
    text_run = element.get('textRun')
    if not text_run:
        return ''
    return text_run.get('content')


def _read_strucutural_elements(elements):
    """Recurses through a list of Structural Elements to read a document's text where text may be
        in nested elements.

        Args:
            elements: a list of Structural Elements.
    """
    text = ''
    for value in elements:
        if 'paragraph' in value:
            elements = value.get('paragraph').get('elements')
            for elem in elements:
                text += _read_paragraph_element(elem)
        elif 'table' in value:
            # The text in table cells are in nested Structural Elements and tables may be
            # nested.
            table = value.get('table')
            for row in table.get('tableRows'):
                cells = row.get('tableCells')
                for cell in cells:
                    text += _read_strucutural_elements(cell.get('content'))
        elif 'tableOfContents' in value:
            # The text in the TOC is also in a Structural Element.
            toc = value.get('tableOfContents')
            text += _read_strucutural_elements(toc.get('content'))
    return text


def _get_google_doc_text(file_id, token):
    creds = Credentials.from_authorized_user_info({
        "refresh_token": token.encrypted_token,
        "client_id": os.environ["CLIENT_ID"],
        "client_secret": os.environ["CLIENT_SECRET"],
        "scopes": ["https://www.googleapis.com/auth/drive.file"]
    })
    creds.refresh(Request())
    service = build("docs", "v1", credentials=creds)
    doc = service.documents().get(documentId=file_id).execute()
    doc_content = doc.get('body').get('content')
    text = _read_strucutural_elements(doc_content)
    return text


def _get_gdrive_text(file_id, token):
    creds = Credentials.from_authorized_user_info({
        "refresh_token": token.encrypted_token,
        "client_id": os.environ["CLIENT_ID"],
        "client_secret": os.environ["CLIENT_SECRET"],
        "scopes": ["https://www.googleapis.com/auth/drive.file"]
    })
    creds.refresh(Request())
    service = build("drive", "v3", credentials=creds)
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
    fh.seek(0)
    text = fh.read().decode("UTF-8")
    return text


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
        

        if mimetype == "text/plain":
            bot = installation_store.find_bot(
                enterprise_id=None,
                team_id=team,
            )
            token = bot.bot_token
            text = _get_txt_document_text(token, url)
        elif mimetype == "application/pdf":
            bot = installation_store.find_bot(
                enterprise_id=None,
                team_id=team,
            )
            token = bot.bot_token
            text = _get_pdf_document_text(token, url)
        elif filetype == "docx" and converted_pdf is not None:
            bot = installation_store.find_bot(
                enterprise_id=None,
                team_id=team,
            )
            token = bot.bot_token
            text = _get_pdf_document_text(token, url)
        elif filetype == "notion":
            db = SessionLocal()
            notion_token = db.query(NotionToken).filter(NotionToken.user_id == user).first().encrypted_token
            db.close()
            text = _get_notion_document_text(file_id, notion_token)
            if not text:
                continue
        elif filetype == "drive#file|application/pdf":
            db = SessionLocal()
            google_token = db.query(GoogleToken).filter(GoogleToken.user_id == user).first()
            db.close()
            text = _get_gdrive_pdf_text(file_id, google_token)
        elif filetype == "drive#file|application/vnd.google-apps.document":
            db = SessionLocal()
            google_token = db.query(GoogleToken).filter(GoogleToken.user_id == user).first()
            db.close()
            text = _get_google_doc_text(file_id, google_token)
        elif filetype == "drive#file|text/plain":
            db = SessionLocal()
            google_token = db.query(GoogleToken).filter(GoogleToken.user_id == user).first()
            db.close()
            text = _get_gdrive_text(file_id, google_token)
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
            "type": filetype or mimetype
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