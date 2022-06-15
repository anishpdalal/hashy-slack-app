import json
import logging
import os

import openai
import pinecone
from sentence_transformers import SentenceTransformer
from transformers import AutoTokenizer, AutoModelForSequenceClassification, TextClassificationPipeline


logger = logging.getLogger()
logger.setLevel(logging.INFO)

search_model = SentenceTransformer("/mnt/bi_encoder")
tokenizer = AutoTokenizer.from_pretrained("/mnt/tokenizer")
model = AutoModelForSequenceClassification.from_pretrained("/mnt/intention_model")
pipe = TextClassificationPipeline(model=model, tokenizer=tokenizer)
openai.api_key = os.getenv("OPENAI_API_KEY")

PINECONE_KEY = os.environ["PINECONE_KEY"]
pinecone.init(api_key=PINECONE_KEY, environment="us-west1-gcp")
index = pinecone.Index(index_name="semantic-text-search")


def _convert_date_to_str(d):
    if d.__class__.__name__ == "date" or d.__class__.__name__ == "datetime":
      return str(d)
    return d


def _search_documents(team, embedding, k=10):
    filter = {"team_id": {"$eq": team}}
    query_results = index.query(
        queries=[embedding.tolist()],
        top_k=k,
        filter=filter,
        include_metadata=True
    )
    results = []
    matches = query_results["results"][0]["matches"]
    for match in matches:
        metadata = match["metadata"]
        if match["score"] >= 0.3:
            results.append({
                "id": match["id"],
                "name": _convert_date_to_str(metadata["source_name"]),
                "url": metadata.get("url"),
                "text": metadata.get("text"),
                "text_type": metadata.get("text_type"),
                "last_updated": metadata["last_updated"].strftime("%m/%d/%Y"),
                "semantic_score": match["score"],
                "source_type": metadata.get("source_type"),
                "answer": metadata.get("answer"),
                "source_id": metadata["source_id"],
            }) 
    return results


def handler(event, context):
    path = event["path"]
    body = json.loads(event["body"]) if event.get("body") else {}
    logger.info(body)
    results = {
        "statusCode": 200,
        "body": {},
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Content-Type": "application/json"
        }
    }
    if path == "/ping":
        return results
    elif path == "/search":
        team = body["team_id"]
        query = body["query"]
        event_type = body.get("event_type")
        results["body"]["query"] = query
        results["body"]["modified_query"] = None
        results["body"]["query_id"] = body.get("query_id")
        results["body"]["results"] = None
        if event_type == "CHANNEL_SEARCH":
            pred = pipe(query, truncation=True, max_length=512)[0]
            label = pred["label"]
            intention_score = pred["score"]
            logger.info({
                "team_id": team,
                "user_id": body["user_id"],
                "query": query,
                "query_id": body.get("query_id"),
                "event_type": "INTENTION_CLASSIFICATION",
                "score": intention_score,
                "label": label,
            })
            if label == "LABEL_0":
                return json.dumps(results["body"])
            response = openai.Completion.create(
                engine="text-davinci-002",
                prompt=f"Convert the question into a search query\n\nQuestion: I had a customer who called in a panic because she felt like her car would not be covered as it falls into the exotic car part of our policy. Is that covered?\nSearch Query: are exotic cars covered?\n\nQuestion: {query}\nSearch Query:",
                temperature=0,
                max_tokens=64,
                top_p=1,
                frequency_penalty=0,
                presence_penalty=0
            )
            query = response["choices"][0]["text"].strip()
            results["body"]["modified_query"] = query
        query_embedding = search_model.encode([query])
        search_documents = _search_documents(team, query_embedding, k=20)
        results["body"]["results"] = search_documents
        logger.info({
            "team_id": team,
            "user_id": body["user_id"],
            "query": query,
            "query_id": body.get("query_id"),
            "event_type": "PREDICTION",
            "top_result_score": search_documents[0]["semantic_score"] if search_documents else None,
            "top_result_id": search_documents[0]["id"] if search_documents else None
        })
        results["body"]["results"] = search_documents
        results["body"] = json.dumps(results["body"])
    return results