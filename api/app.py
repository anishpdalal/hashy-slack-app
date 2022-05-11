import json
import logging
import os

import numpy as np
import openai
import pinecone
from sentence_transformers import CrossEncoder, SentenceTransformer


logger = logging.getLogger()
logger.setLevel(logging.INFO)

search_model = SentenceTransformer(os.environ["DATA_DIR"])
cross_encoder = CrossEncoder(os.environ["CROSS_ENCODER_DIR"])
openai.api_key = os.getenv("OPENAI_API_KEY")

PINECONE_KEY = os.environ["PINECONE_KEY"]
pinecone.init(api_key=PINECONE_KEY, environment="us-west1-gcp")
index = pinecone.Index(index_name="semantic-text-search")


def _search_slack(team, query, embedding):
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
    cross_inp = [[query, match["metadata"]["text"]] for match in matches]
    cross_scores = cross_encoder.predict(cross_inp)
    for idx, score in enumerate(cross_scores):
        matches[idx]["reranked_score"] = float(score)
    reranked_matches = [x for x in sorted(matches, key=lambda x: x["reranked_score"], reverse=True)]
    for match in reranked_matches:
        metadata = match["metadata"]
        if match["reranked_score"] >= 1:            
            results.append({
                "id": match["id"],
                "name": metadata["source_name"],
                "url": metadata.get("url"),
                "text": metadata["text"],
                "last_updated": metadata["last_updated"].strftime("%m/%d/%Y"),
                "reranked_score": match["reranked_score"],
                "semantic_score": match["score"],
                "source_type": metadata["source_type"],
                "answer": metadata.get("answer"),
                "source_id": metadata["source_id"]
            })
    return results


def _convert_date_to_str(d):
    if d.__class__.__name__ == "date" or d.__class__.__name__ == "datetime":
      return str(d)
    return d


def _search_documents(team, query, embedding, text_type="content"):
    filter = {
        "team_id": {"$eq": team},
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
    cross_inp = [[query, match["metadata"]["text"]] for match in matches]
    cross_scores = cross_encoder.predict(cross_inp)
    for idx, score in enumerate(cross_scores):
        matches[idx]["reranked_score"] = float(score)
    reranked_matches = [x for x in sorted(matches, key=lambda x: x["reranked_score"], reverse=True)]
    for match in reranked_matches:
        metadata = match["metadata"]
        if match["reranked_score"] >= 0.65:
           results.append({
                "id": match["id"],
                "name": _convert_date_to_str(metadata["source_name"]),
                "url": metadata["url"],
                "text": metadata["text"],
                "last_updated": metadata["last_updated"].strftime("%m/%d/%Y"),
                "reranked_score": match["reranked_score"],
                "semantic_score": match["score"],
                "source_type": metadata["source_type"],
                "source_id": metadata["source_id"]
            }) 
    return results


def _get_answer_from_doc(text, query):
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
    answer = response.choices[0]["text"].strip()
    return answer


def _extract_surrounding_text(result):
    try:
        id = result["id"]
        prefix = "-".join(id.split("-")[0:-1])
        suffix = id.split("-")[-1]
        position = int(suffix)
        start = position - 1 if position != 0 else position
        end = position + 1
        ids = [f"{prefix}-{idx}" for idx in range(start, end+1)]
        vectors = index.fetch(ids)["vectors"]
        text = "\n".join([vectors.get(id, {}).get("metadata", {}).get("text", "") for id in ids])
        return text
    except Exception as e:
        logger.error(e)
        return result["text"]


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
        team = body["team"]
        query = body["query"]
        search_type = body.get("search_type")
        results["body"]["query"] = query
        results["body"]["modified_query"] = None
        results["body"]["query_id"] = body.get("query_id")
        if search_type == "channel":
            res = openai.Completion.create(
                model="curie:ft-personal-2022-05-10-21-11-23",
                prompt=query + '\n\n###\n\n',
                max_tokens=1,
                temperature=0,
                logprobs=2
            )
            decision = res['choices'][0]['text'].strip()
            if decision == "reject":
                return results
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
        results["body"]["slack_messages_results"] = _search_slack(team, query, query_embedding)
        results["body"]["content_results"] = _search_documents(team, query, query_embedding, text_type="content")
        if results["body"]["content_results"]:
            top_result = results["body"]["content_results"][0]
            top_result_text = _extract_surrounding_text(top_result)
            results["body"]["summarized_result"] = _get_answer_from_doc(top_result_text, query)
        logger.info(results["body"])
        results["body"] = json.dumps(results["body"])
    return results