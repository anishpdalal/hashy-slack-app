import io
import itertools
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
import openai
import pdfminer.high_level
import requests
from sentence_transformers import SentenceTransformer, util
from slack_sdk.oauth.installation_store.sqlalchemy import SQLAlchemyInstallationStore
from slack_sdk.web import WebClient
from sqlalchemy import create_engine, Column, Integer, PickleType, String, Text, DateTime, log
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
from sqlalchemy.ext.declarative import as_declarative, declared_attr
from sqlalchemy_utils import EncryptedType
from sqlalchemy_utils.types.encrypted.encrypted_type import AesEngine


logger = logging.getLogger()
logging.getLogger("pdfminer").setLevel(logging.WARNING)
logger.setLevel(logging.INFO)

user = os.environ["POSTGRES_USER"]
password = os.environ["POSTGRES_PASSWORD"]
host = os.environ["POSTGRES_HOST"]
database = os.environ["POSTGRES_DB"]
port = os.environ["POSTGRES_PORT"]

SQLALCHEMY_DATABASE_URL = f"postgresql://{user}:{password}@{host}:{port}/{database}"
engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
installation_store = SQLAlchemyInstallationStore(
    client_id=os.environ["SLACK_CLIENT_ID"],
    engine=engine
)

REGEX_EXP = r"(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|\?)\s"
search_model = SentenceTransformer(os.environ["DATA_DIR"])
openai.api_key = os.getenv("OPENAI_API_KEY")

DOCS = {}

@as_declarative()
class Base:
    id: Any
    __name__: str
    # Generate __tablename__ automatically

    @declared_attr
    def __tablename__(cls) -> str:
        return cls.__name__.lower()


class Query(Base):
    id = Column(Integer, primary_key=True, index=True)
    team = Column(String, nullable=False)
    text = Column(Text, nullable=False)
    user = Column(String, nullable=False)
    embedding = Column(PickleType, nullable=False)
    result = Column(Text, nullable=False)
    evidence = Column(Text, nullable=True)
    time_created = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    time_updated = Column(DateTime(timezone=True), onupdate=func.now())


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


def _get_queries(db, team):
    queries = db.query(Query).filter(Query.team == team).all()
    return queries


def _get_most_similar_query(db, team, embedding):
    bot = installation_store.find_bot(
        enterprise_id=None,
        team_id=team,
    )
    queries = _get_queries(db, team)
    if len(queries) == 0:
        return []
    scores = [
        util.semantic_search(embedding, pickle.loads(obj.embedding), top_k=1)[0][0] for obj in queries
    ]
    results = []
    sorted_idx = sorted(range(len(scores)), key=lambda x: scores[x]["score"], reverse=True)
    for idx in sorted_idx:
        score = scores[idx]["score"]
        if score >= 0.4:
            obj = queries[idx]
            user = obj.user
            client = WebClient(token=bot.bot_token)
            result = client.users_info(
                user=user
            )
            user_name = result['user']['name']
            last_modified = obj.time_updated if obj.time_updated else obj.time_created
            results.append({
                "name": user_name,
                "team": team,
                "text": obj.text,
                "source": obj.evidence,
                "last_modified": f"{last_modified.month}/{last_modified.day}/{last_modified.year}",
                "result": obj.result
            })
    return results


def _get_documents(db, team):
    docs = db.query(Document).filter(Document.team == team).all()
    return docs


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


def _get_notion_document_text(file_id, user):
    db = SessionLocal()
    try:
        token = db.query(NotionToken).filter(NotionToken.user_id == user).first().encrypted_token
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
    except:
        raise
    finally:
        db.close()
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


def _get_k_most_similar_docs(docs, embedding, user, k=1):
    if len(docs) == 0:
        return
    scores = []
    doc_idx = []
    for idx, obj in enumerate(docs):
        res = util.semantic_search(embedding, pickle.loads(obj.embeddings), top_k=k)[0]
        scores.append(res)
        doc_idx.extend([idx] * len(res))
    scores = list(itertools.chain(*scores))
    sorted_idx = sorted(range(len(scores)), key=lambda x: scores[x]["score"], reverse=True)
    results = []
    for idx in sorted_idx[:k]:
        score = scores[idx]["score"]
        if score >= 0.3:
            doc = docs[doc_idx[idx]]
            name = doc.name
            private_url = doc.url
            team = doc.team
            file_id = doc.file_id
            filetype = doc.type
            if file_id in DOCS:
                text = DOCS[file_id]
            else:
                try:
                    if "notion.so" in doc.url:
                        text = _get_notion_document_text(file_id, user)
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
                    elif name.endswith(".pdf") or name.endswith(".docx"):
                        text = _get_pdf_document_text(private_url, team)
                    else:
                        text = _get_txt_document_text(private_url, team)
                except:
                    continue
                DOCS[file_id] = text

            sentences = re.split(REGEX_EXP, text)
            corpus_id = scores[idx]["corpus_id"]
            if len(sentences) == 1:
                snippet = sentences[corpus_id]
            else:
                snippet = " ".join(sentences[corpus_id:corpus_id+2])
            snippet_processed = " ".join(snippet.split("\n")).strip()
            results.append({
                "source": private_url,
                "name": name,
                "text": None,
                "team": team,
                "last_modified": None,
                "result": snippet_processed
            })
    return results

def _get_summary(text):
    response = openai.Completion.create(
        engine="curie-instruct-beta-v2",
        prompt=f"{text}\n\ntl;dr:",
        temperature=0,
        max_tokens=32,
        top_p=1,
        frequency_penalty=0,
        presence_penalty=0
    )
    summary_text = response.choices[0]["text"].strip()
    summary_text= ".".join(summary_text.split(".")[:-1])
    return summary_text


def handler(event, context):
    path = event["path"]
    body = json.loads(event["body"]) if event.get("body") else {}
    if path == "/ping":
        return {
            "statusCode": 200,
            "body": "pong",
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Content-Type": "application/json"
            }
        }
    elif path == "/search":
        team = body["team"]
        user = body["user"]
        query = body["query"]
        results = {}
        db = SessionLocal()
        try:
            query_embedding = search_model.encode([query])
            results["answers"] = _get_most_similar_query(db, team, query_embedding)
            docs = _get_documents(db, team)
            k = body.get("count", 1)
            results["search_results"] = _get_k_most_similar_docs(docs, query_embedding, user, k=k)
            results["summary"] = _get_summary(results["search_results"][0]["result"])
        except:
            db.rollback()
            raise
        finally:
            db.close()
        return {
            "statusCode": 200,
            "body": json.dumps(results),
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Content-Type": "application/json"
            }
        }
    elif path == "/create-answer":
        team = body["team"]
        user = body["user"]
        text = body["text"]
        text_embedding = search_model.encode([text])
        evidence = body.get("evidence", None)
        result = body["result"]
        query = Query(**{
                "team": team,
                "user": user,
                "text": text,
                "embedding": pickle.dumps(text_embedding),
                "evidence": evidence,
                "result": result
            }
        )
        db = SessionLocal()
        try:
            db.add(query)
            db.commit()
            db.refresh(query)
        except:
            db.rollback()
            raise
        finally:
            db.close()
        return {
            "statusCode": 200,
            "body": "success",
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Content-Type": "application/json"
            }
        }

    else:
        return {
            "statusCode": 200,
            "body": None,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Content-Type": "application/json"
            }
        }
