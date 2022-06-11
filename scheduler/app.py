import datetime
import itertools
import json
import logging
import os

import boto3
import pytz

from core.integration import reader
from core.db import crud

logger = logging.getLogger()
logger.setLevel(logging.INFO)

UPSERT_LIMIT=1000


def chunks(iterable, batch_size=10):
    it = iter(iterable)
    chunk = tuple(itertools.islice(it, batch_size))
    while chunk:
        yield chunk
        chunk = tuple(itertools.islice(it, batch_size))


def handler(event, context):
    sqs = boto3.resource("sqs", region_name="us-east-1")
    queue = sqs.get_queue_by_name(QueueName=os.getenv("SQS_QUEUE_NAME"))
    body = json.loads(event["body"]) if event.get("body") else {}
    event_type = body.get("event_type")
    integrations = crud.get_all_integrations()
    upserts = []
    deletes = []
    if event_type == "DELETE":
        for integration in integrations:
            deletes.append({
                "MessageBody": json.dumps({"event_type": "BULK_DELETE", "integration_id": integration.id}),
                "Id": str(integration.id)
            })
        for chunk in chunks(deletes, batch_size=10):
            queue.send_messages(Entries=chunk)
    else:
        for integration in integrations:
            if len(upserts) > UPSERT_LIMIT:
                break
            result = reader.list_content_stores(integration)
            cursor = result["cursor"]
            crud.update_integration(integration.id, {"last_cursor": cursor})
            content_stores = result["content_stores"]
            for content_store in content_stores:
                type = content_store["type"]
                last_updated = content_store["source_last_updated"]
                source_id = content_store["source_id"]
                content_store_db = crud.get_content_store(source_id)
                if not content_store_db and type == "slack_channel":
                    continue
                last_updated = pytz.utc.localize(
                    datetime.datetime.strptime(
                        last_updated,
                        "%Y-%m-%dT%H:%M:%S.%fZ"
                    )
                )
                if content_store_db:
                    last_updated_in_db = content_store_db.updated or content_store_db.created
                    if last_updated_in_db > last_updated:
                        continue
                content_store["integration_id"] = integration.id
                content_store["source_last_updated"] = last_updated.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                upserts.append({
                    "MessageBody": json.dumps(content_store),
                    "Id": f"{content_store['source_id']}-{integration.id}"
                })
        
        logger.info(f"Upserting {len(upserts)} docs")
        for chunk in chunks(upserts, batch_size=10):
            queue.send_messages(Entries=chunk)

    crud.dispose_engine()