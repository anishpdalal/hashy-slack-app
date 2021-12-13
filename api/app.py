import io
import itertools
import json
import logging
import os
import pickle
import re
from typing import Any

import openai
import pdfminer.high_level
import requests
from sentence_transformers import SentenceTransformer, util
from slack_sdk.oauth.installation_store.sqlalchemy import SQLAlchemyInstallationStore
from slack_sdk.web import WebClient
from sqlalchemy import create_engine, Column, Integer, PickleType, String, Text, DateTime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
from sqlalchemy.ext.declarative import as_declarative, declared_attr


logger = logging.getLogger()
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
    access_token = Column(String, nullable=False)
    time_created = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    bot_id = Column(String, nullable=False)
    workspace_id = Column(String, nullable=False)


def _get_queries(db, team):
    queries = db.query(Query).filter(Query.team == team).all()
    return queries


def _get_most_similar_query(queries, embedding):
    if len(queries) == 0:
        return
    scores = [
        util.semantic_search(embedding, pickle.loads(obj.embedding), top_k=1)[0][0] for obj in queries
    ]
    max_idx = max(range(len(scores)), key=lambda x: scores[x]["score"])
    obj = queries[max_idx]
    score = scores[max_idx]["score"]
    if score >= 0.4:
        return obj
    else:
        return


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
        token = db.query(NotionToken).filter(NotionToken.user_id == user).first().access_token
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

            try:
                if name.endswith(".pdf") or name.endswith(".docx"):
                    text = _get_pdf_document_text(private_url, team)
                elif "notion.so" in doc.url:
                    text = _get_notion_document_text(file_id, user)
                else:
                    text = _get_txt_document_text(private_url, team)
            except:
                continue

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
    elif path == "/answer":
        team = body["team"]
        user = body["user"]
        query = body["query"]
        db = SessionLocal()
        try:
            queries = _get_queries(db, team)
            query_embedding = search_model.encode([query])
            msq = _get_most_similar_query(queries, query_embedding)
        except:
            db.rollback()
            raise
        finally:
            db.close()
        if msq:
            user = msq.user
            bot = installation_store.find_bot(
                enterprise_id=None,
                team_id=team,
            )
            client = WebClient(token=bot.bot_token)
            result = client.users_info(
                user=user
            )
            user_name = result['user']['name']
            last_modified = msq.time_updated if msq.time_updated else msq.time_created
            results = [{
                "user": user_name,
                "team": team,
                "text": msq.text,
                "source": msq.evidence,
                "last_modified": f"{last_modified.month}/{last_modified.day}/{last_modified.year}",
                "result": msq.result
            }]
        else:
            db = SessionLocal()
            try:
                docs = _get_documents(db, team)
                results = _get_k_most_similar_docs(docs, query_embedding, user)
            except:
                db.rollback()
                raise
            finally:
                db.close()
            if len(results) == 1:
                snippet_processed = results[0]["result"]
                response = openai.Completion.create(
                    engine="curie",
                    prompt=f"Original: The Company and the Founders will provide the Investors with customary representations and warranties examples of which are set out in Appendix 4 and the Founders will provide the Investors with customary non-competition, non-solicitation and confidentiality undertakings.\nSummary: The Company and its Founders will provide the usual assurances and guarantees on facts about the business. The founders will also agree not to work for competitors, poach employees or customers when they leave the startup, and respect confidentiality.\n###\nOriginal: One immediately obvious and enormous area for Bitcoin-based innovation is international remittance. Every day, hundreds of millions of low-income people go to work in hard jobs in foreign countries to make money to send back to their families in their home countries – over $400 billion in total annually, according to the World Bank.\nSummary: Bitcoin can be an innovation for sending money overseas. The market opportunity is large. Workers send over $400 billion annually to their families in their home countries. \n###\nOriginal: In the event of an initial public offering of the Company's shares on a US stock exchange the Investors shall be entitled to registration rights customary in transactions of this type (including two demand rights and unlimited shelf and piggy-back rights), with the expenses paid by the Company.\nSummary: If the Company does an IPO in the USA, investors have the usual rights to include their shares in the public offering and the costs of d doing this will be covered by the Company.\n###\nOriginal: Finally, a fourth interesting use case is public payments. This idea first came to my attention in a news article a few months ago. A random spectator at a televised sports event held up a placard with a QR code and the text “Send me Bitcoin!” He received $25,000 in Bitcoin in the first 24 hours, all from people he had never met. This was the first time in history that you could see someone holding up a sign, in person or on TV or in a photo, and then send them money with two clicks on your smartphone: take the photo of the QR code on the sign, and click to send the money.\nSummary: Public payments is an interesting use case for Bitcoin. A person collected $25,000 in Bitcoin from strangers after holding up a QR code. It was the first time in history such an event occured.\n###\nOriginal: {snippet_processed}\n",
                    temperature=0,
                    max_tokens=32,
                    top_p=1,
                    frequency_penalty=1,
                    presence_penalty=0,
                    stop=["\n"]
                )
                try:
                    answer = response["choices"][0]["text"].split("Summary: ")[1]
                    answer = ".".join(answer.split(".")[:-1])
                    results[0]["result"] = answer
                except:
                    results[0]["result"] = snippet_processed
        return {
            "statusCode": 200,
            "body": json.dumps(results),
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Content-Type": "application/json"
            }
        }
    
    elif path == "/search":
        team = body["team"]
        user = body["user"]
        db = SessionLocal()
        try:
            docs = _get_documents(db, team)
            k = body.get("count", 1)
            query = body["query"]
            query_embedding = search_model.encode([query])
            results = _get_k_most_similar_docs(docs, query_embedding, user, k=k)
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
