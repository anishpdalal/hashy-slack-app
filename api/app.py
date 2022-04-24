
import json
import logging
import os

import openai
import pinecone
from sentence_transformers import SentenceTransformer


logger = logging.getLogger()
logger.setLevel(logging.INFO)

search_model = SentenceTransformer(os.environ["DATA_DIR"])
openai.api_key = os.getenv("OPENAI_API_KEY")

PINECONE_KEY = os.environ["PINECONE_KEY"]
pinecone.init(api_key=PINECONE_KEY, environment="us-west1-gcp")
index = pinecone.Index(index_name="semantic-text-search")


def _search_slack(team, embedding):
    filter = {
        "team_id": {"$eq": team},
        "text_type": {"$eq": "content"},
        "$or": [
            {"source_type": {"$eq": "slack_message"}},
            {"source_type": {"$eq": "answer"}},
        ]
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
        metadata = match["metadata"]
        if match["score"] >= 0.3:            
            results.append({
                "id": match["id"],
                "name": metadata["source_name"],
                "url": metadata["url"],
                "text": metadata["text"],
                "last_updated": metadata["last_updated"].strftime("%m/%d/%Y"),
                "semantic_score": match["score"],
                "source_type": metadata["source_type"]
            })
    return results


def _convert_date_to_str(d):
    if d.__class__.__name__ == "date" or d.__class__.__name__ == "datetime":
      return str(d)
    return d


def _search_documents(team, embedding, text_type="content"):
    filter = {
        "team": {"$eq": team},
        "text_type": {"$eq": text_type},
        "$and": [
            {"source_type": {"$ne": "slack_message"}},
            {"source_type": {"$ne": "answer"}},
        ]
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
        metadata = match["metadata"]
        if match["score"] >= 0.3:
           results.append({
                "id": match["id"],
                "name": _convert_date_to_str(metadata["source_name"]),
                "url": metadata["url"],
                "text": metadata["text"],
                "last_updated": metadata["last_updated"].strftime("%m/%d/%Y"),
                "semantic_score": match["score"],
                "source_type": metadata["source_type"]
            }) 
    return results


def _get_summary(text, query):
    if not query.endswith("?"):
        query = f"{query}?"
    prompt = "Answer the question based on the context below, and if the question can't be answered based on the context, say \"I don't know\"\n\nContext:\n{0}\n\n---\n\nQuestion: {1}\nAnswer:"
    response = openai.Completion.create(
        engine="text-davinci-002",
        prompt=prompt.format(text, query),
        temperature=0,
        max_tokens=100,
        top_p=1,
        frequency_penalty=0,
        presence_penalty=0
    )
    summary_text = response.choices[0]["text"].strip()
    return summary_text


def handler(event, context):
    path = event["path"]
    body = json.loads(event["body"]) if event.get("body") else {}
    logger.info(body)
    results = {
        "statusCode": 200,
        "body": None,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Content-Type": "application/json"
        }
    }
    if path == "/ping":
        return results
    elif path == "/search":
        team = body["team"]
        query = body["query"]
        search_type = body["search_type"]
        results_body = {
            "query": query,
            "modified_query": None,
            "summarized_result": None,
            "slack_messages_results": [],
            "content_results": [],
            "title_results": []
        }
        if search_type == "auto_reply":
            response = openai.Completion.create(
                engine="text-davinci-002",
                prompt=f"Summarize the following into a question\n\n{query}",
                temperature=0,
                max_tokens=64,
                top_p=1,
                frequency_penalty=0,
                presence_penalty=0
            )
            query = response["choices"][0]["text"].strip()
            results_body["modified_query"] = query
        query_embedding = search_model.encode([query])
        results["slack_messages_results"] = _search_slack(team, query_embedding)
        results["content_results"] = _search_documents(team, query_embedding, text_type="content")
        results["title_results"] = _search_documents(team, query_embedding, text_type="title")
        if results["content_results"]:
            results["summarized_result"] = _get_summary(results["content_results"][0]["text"], query)
    
    return results