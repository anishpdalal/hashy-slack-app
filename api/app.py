from datetime import datetime
import difflib
import json
import logging
import os
import re
from typing import Any
import uuid

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import openai
import pandas as pd
import pinecone
from sentence_transformers import SentenceTransformer
from slack_sdk.oauth.installation_store.sqlalchemy import SQLAlchemyInstallationStore
from slack_sdk.web import WebClient
from sqlalchemy import create_engine, Column, Integer, String, DateTime, or_
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
from sqlalchemy.ext.declarative import as_declarative, declared_attr
from sqlalchemy_utils import EncryptedType
from sqlalchemy_utils.types.encrypted.encrypted_type import AesEngine
import sqlite3


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
    query_id = Column(String)
    team = Column(String, nullable=False)
    user = Column(String, nullable=False)
    channel = Column(String)
    time_created = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    time_updated = Column(DateTime(timezone=True), onupdate=func.now())


class GoogleToken(Base):
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(String, nullable=False)
    team = Column(String, nullable=False)
    encrypted_token = Column(EncryptedType(String, os.environ["TOKEN_SEC_KEY"], AesEngine, "pkcs5"))
    time_created = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    channel_id = Column(String)


def _get_most_similar_query(team, embedding):
    filter = {
        "team": {"$eq": team},
        "type": {"$eq": "answer"}
    }
    query_results = index.query(
        queries=[embedding.tolist()],
        top_k=10,
        filter=filter,
        include_metadata=True
    )
    results = []
    matches = query_results["results"][0]["matches"]
    for match in matches:
        if match["score"] >= 0.4:
            results.append({
                "id": match["id"],
                "name": match["metadata"]["name"],
                "team": team,
                "text": match["metadata"]["text"],
                "last_modified": match["metadata"]["last_modified"].strftime("%m/%d/%Y"),
                "result": match["metadata"]["result"]
            })
    return results


def _get_k_most_similar_docs(team, embedding, k=1, file_type=None):
    if file_type:
        filter = {
            "team": {"$eq": team},
            "type": {"$eq": file_type}
        }
    else:
        filter = {
            "team": {"$eq": team},
            "type": {"$ne": "answer"}
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

def _get_summary(text, query):
    if not query.endswith("?"):
        query = f"{query}?"
    response = openai.Completion.create(
        engine="text-davinci-001",
        prompt=f"Below is a prompt\n\n{text}\n\nIf the answer is unknown, say \"Unknown\"\n\nBased on the provided prompt, answer the question {query}",
        temperature=0,
        max_tokens=32,
        top_p=1,
        frequency_penalty=0,
        presence_penalty=0
    )
    summary_text = response.choices[0]["text"].strip()
    if "." in summary_text:
        summary_text= ".".join(summary_text.split(".")[:-1])
    return summary_text


def snake_case(s):
  return '_'.join(
    re.sub('([A-Z][a-z]+)', r' \1',
    re.sub('([A-Z]+)', r' \1',
    s.replace('-', ' '))).split()).lower()


def search(regex: str, df, case=False):
    textlikes = df.select_dtypes(include=[object, "string"])
    return df[
        textlikes.apply(
            lambda column: column.str.contains(regex, regex=True, case=case, na=False)
        ).any(axis=1)
    ]


def get_overlap(s1, s2):
    s = difflib.SequenceMatcher(None, s1, s2)
    pos_a, pos_b, size = s.find_longest_match(0, len(s1), 0, len(s2)) 
    return s1[pos_a:pos_a+size]


def handler(event, context):
    path = event["path"]
    body = json.loads(event["body"]) if event.get("body") else {}
    logger.info(body)
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
        results = {
            "summary": None,
            "answers": [],
            "search_results": []
        }
        db = SessionLocal()
        try:
            query_embedding = search_model.encode([query])
            results["answers"] = _get_most_similar_query(team, query_embedding)
            k = body.get("count", 1)
            results["search_results"] = _get_k_most_similar_docs(team, query_embedding, k=k)
            if results["search_results"]:
                results["summary"] = _get_summary(results["search_results"][0]["result"], query)
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
        results["search_results"] = _get_k_most_similar_docs(team, query_embedding, k=k, file_type=file_type)
        if results["search_results"]:
            file_url = results["search_results"][0]["source"]
            result = results["search_results"][0]["result"]
            title = results["search_results"][0]["name"]
            sheet_range = result.split(f"{title} - ")[1].strip(".")
            db = SessionLocal()
            token = db.query(GoogleToken).filter(or_(GoogleToken.user_id == user)).first()
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
            rows = gsheet.spreadsheets().values().get(spreadsheetId=file_id, range=sheet_range).execute()
            columns = rows["values"][0]
            data = []
            n = len(columns)
            for row in rows["values"][1:]:
                data_row = row[0:n] + [None] * (n - len(row))    
                data.append(data_row)
            df = pd.DataFrame(data=data, columns=columns)
            df.columns = [snake_case(x) for x in df.columns]
            df = df.loc[:,~df.columns.duplicated()].copy()
            for column in df.columns:
                try:
                    df[column] = df[column].replace("", None).apply(lambda x: int(x.replace(",","")))
                    continue
                except:
                    pass
                try:
                    df[column] = df[column].replace("", None).apply(lambda x: float(x.replace(",","")))
                    continue
                except:
                    pass
                try:
                    df[column] = pd.to_datetime(df[column])
                    df[column] = df[column].dt.strftime('%Y-%m-%d')
                    continue
                except:
                    pass
                try:
                    df[column] = df[column].str.lower()
                    continue
                except:
                    pass
            nunique = df.nunique()
            cols_to_drop = nunique[nunique == 1].index
            df.drop(cols_to_drop, axis=1, inplace=True)
            cnx = sqlite3.connect(':memory:')
            df.to_sql(name='t', con=cnx)
            response = openai.Completion.create(
                engine="code-davinci-001",
                prompt=f"###Postgres table, with its properties:\n#\n# t({', '.join(df.columns)})\n#\n### A query to {question}\nSELECT",
                temperature=0,
                max_tokens=100,
                top_p=1,
                frequency_penalty=0,
                presence_penalty=0,
                stop=["#", ";"]
            )
            sql = response["choices"][0]["text"].strip(" ").lower()
            sql_query = f"select {sql}"
            filters = re.findall(r"'(.*?)'", sql_query, re.DOTALL)
            try:
                result = pd.read_sql(sql_query, cnx)
                if len(result) == 0:
                    filtered_df = df
                    for filter in filters:
                        tmp_df = search(filter, filtered_df)
                        if len(tmp_df) > 0:
                            filtered_df = tmp_df
                        result = filtered_df
            except:
                filtered_df = df
                for filter in filters:
                    tmp_df = search(filter, filtered_df)
                    if len(tmp_df) > 0:
                        filtered_df = tmp_df
                result = filtered_df
        if len(result) == len(df):
            result = pd.DataFrame(columns=df.columns, data=[])
        keys = list(json.loads(result.to_json()).keys())
        values = list(zip(*[d.values() for d in list(json.loads(result.to_json()).values())]))
        summary = []
        for val in values:
            tmp = {}
            for i in range(len(val)):
                tmp[keys[i]] = val[i]
            summary.append(tmp)
        if len(summary) == 0:
            try:
                question_mod = question.replace("?","").lower()
                candidates = {q.strip() for q in question_mod.split(" ")}
                for val in rows["values"]:
                    striped = " ".join(val).lower().strip()
                    overlap = get_overlap(striped, question_mod).strip()
                    if overlap in candidates or any(item in overlap for item in candidates):
                        summary.append([i for i in val if i])
                summary = "\n".join([" | ".join(row) for row in summary])[0:3000]
            except:
                pass
        results["summary"] = summary
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
        result = body["result"]
        text_embedding = search_model.encode([text]).tolist()
        query_id = str(uuid.uuid4())
        query = Query(**{
                "team": team,
                "user": user,
                "query_id": query_id
            }
        )
        db = SessionLocal()
        try:
            db.add(query)
            db.commit()
            db.refresh(query)
            bot = installation_store.find_bot(
                enterprise_id=None,
                team_id=team,
            )
            client = WebClient(token=bot.bot_token)
            user_name = client.users_info(user=user)['user']['name']
            last_modified = datetime.now().strftime("%m/%d/%Y")
            index.upsert(vectors=zip(
                [query_id],
                text_embedding,
                [
                    {
                        "user": user,
                        "team": team,
                        "text": text,
                        "result": result,
                        "type": "answer",
                        "name": user_name,
                        "last_modified": last_modified
                    }
                ]
            ))
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
    elif path == "/list-answers":
        team = body["team"]
        user = body["user"]
        db = SessionLocal()
        queries = db.query(Query).filter(Query.user == user, Query.team == team).all()
        db.close()
        query_ids = [q.query_id for q in queries]
        results = []
        if query_ids:
            vectors = index.fetch(query_ids)["vectors"]
            for query in queries:
                if query.query_id in vectors:
                    results.append({
                        "query_id": query.query_id,
                        "question": vectors.get(query.query_id)["metadata"]["text"],
                        "answer": vectors.get(query.query_id)["metadata"]["result"]
                    })
        return {
            "statusCode": 200,
            "body": json.dumps(results),
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Content-Type": "application/json"
            }
        }
    elif path == "/delete-answers":
        team = body["team"]
        user = body["user"]
        db = SessionLocal()
        query_ids = body["query_ids"]
        for query_id in query_ids:
            db.query(Query).filter(Query.user == user, Query.team == team, Query.query_id == query_id).delete()
        db.commit()
        index.delete(ids=query_ids)
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
