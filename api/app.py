import datetime
import functools
import json
import logging
import os
import pickle
from typing import Any

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import numpy as np
import openai
import pandas as pd
import pinecone
from sentence_transformers import SentenceTransformer, util
from slack_sdk.oauth.installation_store.sqlalchemy import SQLAlchemyInstallationStore
from slack_sdk.web import WebClient
from sqlalchemy import create_engine, Column, Integer, PickleType, String, Text, DateTime, or_
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
from sqlalchemy.ext.declarative import as_declarative, declared_attr
from sqlalchemy_utils import EncryptedType
from sqlalchemy_utils.types.encrypted.encrypted_type import AesEngine
from transformers import AutoTokenizer, AutoModelForTableQuestionAnswering


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

PINECONE_KEY = os.environ["PINECONE_KEY"]
pinecone.init(api_key=PINECONE_KEY, environment="us-west1-gcp")
index = pinecone.Index(index_name="semantic-text-search")


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
    embeddings = Column(PickleType)
    num_vectors = Column(Integer)
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


class GoogleToken(Base):
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(String, nullable=False)
    team = Column(String, nullable=False)
    encrypted_token = Column(EncryptedType(String, os.environ["TOKEN_SEC_KEY"], AesEngine, "pkcs5"))
    time_created = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    channel_id = Column(String)


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


def _get_k_most_similar_docs(team, embedding, user, channel, k=1, file_type=None):
    if file_type:
        filter = {
            "team": {"$eq": team},
            "$or": [{"user": {"$eq": user}}, {"channel": {"$eq": channel}}],
            "type": {"$eq": file_type}
        }
    else:
        filter = {
            "team": {"$eq": team},
            "$or": [{"user": {"$eq": user}}, {"channel": {"$eq": channel}}]
        }
    query_results = index.query(
        queries=[embedding.tolist()],
        top_k=k,
        filter=filter,
        include_metadata=True
    )
    results = []
    matches = query_results["results"][0]["matches"]
    for match in matches:
        if match["score"] >= 0.3:
            results.append({
                "source": match["metadata"]["url"],
                "name": match["metadata"]["title"],
                "text": None,
                "team": team,
                "last_modified": None,
                "result": match["metadata"]["text"]
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
        channel = body["channel"]
        query = body["query"]
        results = {
            "summary": None,
            "answers": [],
            "search_results": []
        }
        db = SessionLocal()
        try:
            query_embedding = search_model.encode([query])
            results["answers"] = _get_most_similar_query(db, team, query_embedding)
            k = body.get("count", 1)
            results["search_results"] = _get_k_most_similar_docs(team, query_embedding, user, channel, k=k)
            if results["search_results"]:
                results["summary"] = _get_summary(results["search_results"][0]["result"])
            else:
                results["summary"] = None
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
    elif path == "/tabular-search":
        team = body["team"]
        user = body["user"]
        channel = body["channel"]
        results = {
            "summary": None,
            "answers": [],
            "search_results": []
        }
        split_query = body["query"].split("|")
        if len(split_query) != 2:
            return {
                "statusCode": 200,
                "body": json.dumps(results),
                "headers": {
                    "Access-Control-Allow-Origin": "*",
                    "Content-Type": "application/json"
                }
            }
        question = body["query"].split("|")[0]
        query = body["query"].split("|")[1]
        query_embedding = search_model.encode([query])
        k = body.get("count", 1)
        file_type = "drive#file|application/vnd.google-apps.spreadsheet"
        results["search_results"] = _get_k_most_similar_docs(team, query_embedding, user, channel, k=k, file_type=file_type)
        if results["search_results"]:
            file_url = results["search_results"][0]["source"]
            db = SessionLocal()
            token = db.query(GoogleToken).filter(or_(GoogleToken.user_id == user, GoogleToken.channel_id == channel)).first()
            db.close()
            creds = Credentials.from_authorized_user_info({
                "refresh_token": token.encrypted_token,
                "client_id": os.environ["CLIENT_ID"],
                "client_secret": os.environ["CLIENT_SECRET"],
                "scopes": ["https://www.googleapis.com/auth/drive.file"]
            })
            creds.refresh(Request())
            file_id = file_url.split("/")[-1]
            gsheet = build("sheets", "v4", credentials=creds)
            rows = gsheet.spreadsheets().values().get(spreadsheetId=file_id, range="A1:Z100").execute()
            columns = rows["values"][0]
            column_embeddings = search_model.encode(columns)
            ids = [c["corpus_id"] for c in util.semantic_search(query_embedding, column_embeddings, top_k=5)[0]]
            n = len(columns)
            data = []
            for row in rows["values"][1:]:
                data.append(row[0:n])
            df = pd.DataFrame(data=data, columns=columns).fillna("N/A").iloc[:, ids]
            question_search = "|".join("".join([i.lower() for i in question if i != "?"]).split(" "))
            question_search = r"\b(" + question_search + r")\b"
            mask = functools.reduce(np.logical_or, [df[col].str.lower().str.contains(question_search) for col in df.columns])
            masked_df = df.loc[mask].reset_index().drop("index", 1)
            if len(masked_df) == 0:
                masked_df = df
            model = AutoModelForTableQuestionAnswering.from_pretrained("/mnt/table_qa")
            tokenizer = AutoTokenizer.from_pretrained("/mnt/table_qa")
            queries = [question]
            inputs = tokenizer(table=masked_df, queries=queries, padding='max_length', return_tensors="pt")
            outputs = model(**inputs)
            predicted_table_cell_coords, predicted_aggregation_operators = tokenizer.convert_logits_to_predictions(
                inputs,
                outputs.logits.detach(),
                outputs.logits_aggregation.detach()
            )
            aggregation_operators = {0: "NONE", 1: "SUM", 2: "AVERAGE", 3:"COUNT"}
            operator = [aggregation_operators[x] for x in predicted_aggregation_operators][0]
            answers = []
            for coordinates in predicted_table_cell_coords:
                if len(coordinates) == 1:
                    answers.append(masked_df.iat[coordinates[0]])
                else:
                    for coordinate in coordinates:
                        answers.append(masked_df.iat[coordinate])
            try:
                if operator == "NONE":
                    results["summary"] = ", ".join(answers)
                elif operator == "SUM":
                    results["summary"] = sum([float(a) for a in answers])
                elif operator == "AVERAGE":
                    results["summary"] = sum([float(a) for a in answers]) / len(answers)
                elif operator == "COUNT":
                    results["summary"] = len(answers)
                else:
                    pass
            except:
                results["summary"] = f"{operator}: {', '.join(answers)}" if answers else "N/A"
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
