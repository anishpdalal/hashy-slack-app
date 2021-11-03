import pickle
import os

from fastapi import Depends, FastAPI, Request
from slack_bolt import App
from slack_bolt.adapter.fastapi import SlackRequestHandler
import numpy as np
import requests
import spacy
from sentence_transformers import SentenceTransformer, util

from app.db import crud, database, schemas


app = App()
app_handler = SlackRequestHandler(app)

nlp = spacy.load("en_core_web_sm")
search_model = SentenceTransformer('msmarco-distilbert-base-v4')
db = database.SessionLocal()



@app.event("file_created")
def handle_file_created_events(event, say):
    pass


def _get_document_text(url):
    headers = {
        "Authorization": f"Bearer {os.environ['SLACK_BOT_TOKEN']}"
    }
    text = requests.get(url, headers=headers).text
    return text


def _process_document(file):
    url_private = file["url_private"]
    team= url_private.split("/")[4].split("-")[0]
    text = _get_document_text(url_private)
    doc = nlp(text)
    sentences = []
    word_positions = []
    for sent in doc.sents:
        sentences.append(sent.text)
        start, end = sent.start, sent.end
        word_positions.append(f"{start}_{end}")
    doc_embeddings = search_model.encode(sentences)
    word_positions = "|".join(word_positions)
    doc = schemas.DocumentCreate(
        team=team,
        url=url_private,
        word_positions=word_positions,
        embeddings=pickle.dumps(doc_embeddings)
    )
    crud.create_document(db, doc)


api = FastAPI()

@app.event({
    "type": "message",
    "subtype": "file_share"
})
def handle_message_events(event, say):
    file = event["files"][0]
    if file["mimetype"] == "text/plain":
        _process_document(file)
        say(f"File {file['name']} processed!")


@app.event("message")
def handle_message(event, say):
    event_type = event.get("type")
    text = event.get("text")
    if event_type == "message" and text is not None:
        team = event["team"]
        documents = crud.get_documents(db, team)
        query_embedding = search_model.encode([text])
        scores = [util.semantic_search(query_embedding, pickle.loads(doc.embeddings), top_k=1)[0][0] for doc in documents]
        max_idx = max(range(len(scores)), key=lambda x: scores[x]["score"])
        corpus_id = scores[max_idx]["corpus_id"]
        doc = documents[max_idx]
        start, end = doc.word_positions.split("|")[corpus_id].split("_")
        private_url = doc.url
        text = _get_document_text(private_url)
        snippet = nlp(text)[int(start): int(end)].text
        say(snippet)

api = FastAPI()


@api.post("/slack/events")
async def endpoint(req: Request):
    return await app_handler.handle(req)
