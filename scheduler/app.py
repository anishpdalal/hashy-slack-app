import datetime
import itertools
import json
import logging
import os

import boto3
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import pytz
import requests
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import crud

logger = logging.getLogger()
logger.setLevel(logging.INFO)

pg_user = os.environ["POSTGRES_USER"]
password = os.environ["POSTGRES_PASSWORD"]
host = os.environ["POSTGRES_HOST"]
database = os.environ["POSTGRES_DB"]
port = os.environ["POSTGRES_PORT"]

UPSERT_LIMIT = 1000


def chunks(iterable, batch_size=10):
    it = iter(iterable)
    chunk = tuple(itertools.islice(it, batch_size))
    while chunk:
        yield chunk
        chunk = tuple(itertools.islice(it, batch_size))


def get_notion_search_results(token):
    headers = {
        "Authorization": f"Bearer {token.encrypted_token}",
        "Content-type": "application/json",
        "Notion-Version": "2021-08-16"
    }
    request_body = {
        "sort": {
            "direction": "descending",
            "timestamp": "last_edited_time"
        },
        "filter": {
            "property": "object",
            "value": "page"
        },
    }
    start_cursor = token.last_cursor
    if start_cursor:
        request_body["start_cursor"] = start_cursor
    api_url = "https://api.notion.com/v1/search"
    results = requests.post(
        api_url, headers=headers, data=json.dumps(request_body)
    ).json()
    return results


def get_drive_service(token):
    creds = Credentials.from_authorized_user_info({
        "refresh_token": token.encrypted_token,
        "client_id": os.environ["CLIENT_ID"],
        "client_secret": os.environ["CLIENT_SECRET"],
        "scopes": ["https://www.googleapis.com/auth/drive.file"]
    })
    creds.refresh(Request())
    service = build("drive", "v3", credentials=creds)
    return service


def get_google_search_results(token):
    service = get_drive_service(token)
    start_cursor = token.last_cursor
    if start_cursor:
        results = service.files().list(
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            pageToken=start_cursor,
            fields="nextPageToken, files(id, name, modifiedTime, mimeType)",
            orderBy="modifiedTime desc",
            q="trashed=false" 

        ).execute()
    else:
        results = service.files().list(
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            fields="nextPageToken, files(id, name, modifiedTime, mimeType)",
            orderBy="modifiedTime desc",
            q="trashed=false"
        ).execute()
    return results


def get_tokens(db, user, team):
    tokens = []
    if user and team:
        notion_token = crud.get_notion_token(db, user, team)
        google_token = crud.get_google_token(db, user, team)
        if notion_token:
            tokens.append(notion_token)
        if google_token:
            tokens.append(google_token)
    else:
        tokens.extend(crud.get_notion_tokens(db))
        tokens.extend(crud.get_google_tokens(db))
    return tokens


def get_notion_documents_to_upsert(db, team, user, search_results):
    docs = []
    for result in search_results.get("results", []):
        file_id = result["id"]
        document = crud.get_document(db, file_id)
        last_edited_time = pytz.utc.localize(
            datetime.datetime.strptime(
                result["last_edited_time"],
                "%Y-%m-%dT%H:%M:%S.%fZ"
            )
        )
        last_updated_in_db = None
        if document:
            last_updated_in_db = document.time_updated \
                or document.time_created
        doc_users = document.users if document else []
        # If document hasn't been update since last time and user already
        # has access don't process it
        if last_updated_in_db and last_updated_in_db > last_edited_time \
                and user in doc_users:
            continue
        if result["archived"]:
            continue
        url = result["url"]
        split_url = url.split("/")[-1].split("-")
        if len(split_url) == 1:
            file_name = "Untitled"
        else:
            file_name = " ".join(split_url[:-1])
        doc = {
            "team": team,
            "user": user,
            "url": url,
            "filetype": "notion",
            "file_name": file_name,
            "file_id": result["id"]
        }
        docs.append(doc)
    return docs


def get_google_documents_to_upsert(db, team, user, search_results):
    docs = []
    for result in search_results.get("files", []):
        file_id = result["id"]
        document = crud.get_document(db, file_id)
        last_edited_time = pytz.utc.localize(
            datetime.datetime.strptime(
                result["modifiedTime"],
                "%Y-%m-%dT%H:%M:%S.%fZ"
            )
        )
        last_updated_in_db = None
        if document:
            last_updated_in_db = document.time_updated \
                or document.time_created
        doc_users = document.users if document else []
        # If document hasn't been update since last time and user already
        # has access don't process it
        if last_updated_in_db and last_updated_in_db > last_edited_time \
                and user in doc_users:
            continue
        url = f"https://drive.google.com/file/d/{result['id']}"
        file_name = result["name"]
        doc = {
            "team": team,
            "user": user,
            "url": url,
            "filetype": f"drive#file|{result['mimeType']}",
            "file_name": file_name,
            "file_id": result["id"]
        }
        docs.append(doc)
    return docs


def process(db, token):
    user = token.user_id
    team = token.team
    if type(token).__name__ == "NotionToken":
        search_results = get_notion_search_results(token)
        next_cursor = search_results.get("next_cursor")
        crud.update_last_cursor_notion(db, token.id, next_cursor)
        documents = get_notion_documents_to_upsert(
            db, team, user, search_results
        )
    elif type(token).__name__ == "GoogleToken":
        search_results = get_google_search_results(token)
        next_cursor = search_results.get("nextPageToken")
        crud.update_last_cursor_google(db, token.id, next_cursor)
        documents = get_google_documents_to_upsert(
            db, team, user, search_results
        )
    else:
        documents = []
    return documents


def handler(event, context):
    DB_URL = f"postgresql://{pg_user}:{password}@{host}:{port}/{database}"
    engine = create_engine(DB_URL)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    db = SessionLocal()
    sqs = boto3.resource("sqs", region_name="us-east-1")
    queue = sqs.get_queue_by_name(QueueName=os.getenv("SQS_QUEUE_NAME"))
    if event.get("type") == "INDEX_SLACK_MESSAGES":
        team_ids = crud.get_unique_teams(db)
        teams = [
            {
                "MessageBody": json.dumps({"type": "slack", "team": team}),
                "Id": team
            } for team in team_ids

        ]
        logger.info(f"Processing slack messages for {len(team_ids)} teams")
        for chunk in chunks(teams, batch_size=10):
            queue.send_messages(Entries=chunk)
    else:
        team = event.get("team")
        user = event.get("user")
        upserts = []
        tokens = get_tokens(db, user, team)
        for token in tokens:
            if len(upserts) >= UPSERT_LIMIT:
                break
            documents = process(db, token)
            for doc in documents:
                upserts.append(
                    {
                        "MessageBody": json.dumps(doc),
                        "Id": f"{doc['file_id']}_{token.user_id}"
                    }
                )

        logger.info(f"Upserting {len(upserts)} docs")
        for chunk in chunks(upserts, batch_size=10):
            queue.send_messages(Entries=chunk)

    db.close()
    engine.dispose()
