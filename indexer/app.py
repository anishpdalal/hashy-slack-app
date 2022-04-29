import datetime
import itertools
import json
import logging
import os

import pinecone
import pytz
from sentence_transformers import SentenceTransformer

from core.integration import reader
from core.db import crud

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def chunks(iterable, batch_size=100):
    """A helper function to break an iterable into chunks of size batch_size."""
    it = iter(iterable)
    chunk = tuple(itertools.islice(it, batch_size))
    while chunk:
        yield chunk
        chunk = tuple(itertools.islice(it, batch_size))


def handler(event, context):
    PINECONE_KEY = os.environ["PINECONE_KEY"]
    pinecone.init(api_key=PINECONE_KEY, environment="us-west1-gcp")
    index = pinecone.Index(index_name="semantic-text-search")
    search_model = SentenceTransformer(os.environ["DATA_DIR"])
    for record in event['Records']:
        if isinstance(record["body"], str):
            content_store = json.loads(record["body"])
        else:
            content_store = record["body"]
        logger.info(record['body'])
        user_id = content_store["user_id"]
        team_id = content_store["team_id"]
        source_id = content_store["source_id"]
        content_store_type = content_store["type"]
        content_store_db = None
        source_last_updated = pytz.utc.localize(datetime.datetime.strptime(
            content_store["source_last_updated"],
            "%Y-%m-%dT%H:%M:%S.%fZ"
        ))
        if content_store_type == "answer":
            text = [content_store["text"]]
        else: 
            integration_id = content_store["integration_id"]
            content_store_db = crud.get_content_store(source_id)
            integration = crud.get_integration(integration_id)
            if content_store_type == "slack_channel" and content_store_db:
                db_last_updated = content_store_db.updated or content_store_db.created
                content_store["source_last_updated"] = db_last_updated.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            data = reader.extract_data_from_content_store(integration, content_store)
            text = [d["text"] for d in data]
        if not text:
            continue
        embeddings = search_model.encode(text).tolist()
        num_vectors = content_store_db.num_vectors if content_store_db else 0
        if not content_store_db:
            content = {
                "team_id": team_id,
                "type": content_store_type,
                "source_id": source_id,
                "user_ids": [user_id] if user_id else None,
                "source_last_updated": source_last_updated,
                "num_vectors": len(text)
            }
            crud.create_content_store(content)
        else:
            new_num_vectors = len(text) + num_vectors if content_store_type == "slack_channel" else len(text)
            content = {
                "num_vectors": new_num_vectors,
                "source_last_updated": source_last_updated
            }
            if user_id:
                user_ids = set(content_store_db.user_ids)
                user_ids.add(user_id)
                content["user_ids"] = list(user_ids)
            crud.update_content_store(source_id, content)
        content_store_db = crud.get_content_store(source_id)
        if content_store_type == "answer":
            metadata = {
                "text": content_store["text"],
                "team_id": team_id,
                "text_type": "content",
                "last_updated": content_store["source_last_updated"],
                "source_type": content_store_type,
                "source_name": content_store["source_name"],
                "source_id": user_id,
                "is_boosted": content_store_db.is_boosted,
                "answer": content_store["answer"]

            }
            index.upsert(vectors=[(source_id, embeddings, metadata)])
        else:
            upsert_data_generator = map(lambda i: (
                data[i]["id"],
                embeddings[i],
                {
                    "text": data[i]["text"],
                    "team_id": data[i]["team_id"],
                    "url": data[i]["url"],
                    "text_type": data[i]["text_type"],
                    "last_updated": data[i]["last_updated"],
                    "source_name": data[i]["source_name"],
                    "source_type": data[i]["source_type"],
                    "source_id": data[i]["source_id"],
                    "url": data[i]["url"],
                    "is_boosted": content_store_db.is_boosted

                }), range(len(data))
            )
            if content_store_type == "slack_channel":
                for d in data:
                    slack_message_id = d["id"]
                    slack_message_user = d["user_id"]
                    slack_message_last_updated = pytz.utc.localize(datetime.datetime.strptime(
                        d["last_updated"],
                        "%Y-%m-%dT%H:%M:%S.%fZ"
                    ))
                    if not crud.get_content_store(slack_message_id):
                        content = {
                            "team_id": team_id,
                            "type": "slack_message",
                            "source_id": slack_message_id,
                            "user_ids": [slack_message_user] if slack_message_user else None,
                            "source_last_updated": slack_message_last_updated,
                            "num_vectors": 1
                        }
                        crud.create_content_store(content)
            for ids_vectors_chunk in chunks(upsert_data_generator, batch_size=100):
                index.upsert(vectors=ids_vectors_chunk)
            if (content_store_type != "answer" and content_store_type != "slack_channel") and num_vectors > len(data):
                delete_data_generator = map(lambda i: data[i]["id"], range(len(data), num_vectors))
                for ids_chunk in chunks(delete_data_generator, batch_size=100):
                    index.delete(ids=list(ids_chunk))
        

    crud.dispose_engine()