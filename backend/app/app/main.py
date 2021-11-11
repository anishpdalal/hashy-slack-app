import pickle
import os

from fastapi import FastAPI, Request
from slack_bolt import App
from slack_bolt.adapter.fastapi import SlackRequestHandler
from slack_bolt.oauth.oauth_settings import OAuthSettings
from slack_sdk.oauth.installation_store.sqlalchemy import SQLAlchemyInstallationStore
from slack_sdk import WebClient
import numpy as np
import requests
import spacy
from sentence_transformers import SentenceTransformer, util

from app.db import crud, database, schemas

installation_store = SQLAlchemyInstallationStore(
    client_id=os.environ["SLACK_CLIENT_ID"],
    engine=database.engine
)

installation_store.create_tables()

oauth_settings = OAuthSettings(
    installation_store=installation_store
)

app = App(oauth_settings=oauth_settings)
app_handler = SlackRequestHandler(app)

nlp = spacy.load("en_core_web_sm")
search_model = SentenceTransformer('msmarco-distilbert-base-v4')
db = database.SessionLocal()



@app.event("file_created")
def handle_file_created_events(event, say):
    pass


def _get_document_text(url, team):
    bot = installation_store.find_bot(
        enterprise_id=None,
        team_id=team,
    )
    headers = {
        "Authorization": f"Bearer {bot.bot_token}"
    }
    text = requests.get(url, headers=headers).text
    return text


def _process_document(file):
    url_private = file["url_private"]
    team = url_private.split("/")[4].split("-")[0]
    name = file["name"]
    text = _get_document_text(url_private, team)
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
        name=name,
        url=url_private,
        word_positions=word_positions,
        embeddings=pickle.dumps(doc_embeddings),
        file_id=file["id"],
        user=file["user"]
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


def _get_most_similar_queries(queries, embedding):
    scores = [
        util.semantic_search(embedding, pickle.loads(obj.embedding), top_k=1)[0][0] for obj in queries
    ]
    if len(scores) == 0:
        return
    max_idx = max(range(len(scores)), key=lambda x: scores[x]["score"])
    obj = queries[max_idx]
    score = scores[max_idx]["score"]
    return {
        "obj": obj,
        "score": score,
    }


def _get_most_similar_docs(docs, embedding):
    scores = [
        util.semantic_search(embedding, pickle.loads(obj.embeddings), top_k=1)[0][0] for obj in docs
    ]
    if len(scores) == 0:
        return
    max_idx = max(range(len(scores)), key=lambda x: scores[x]["score"])
    obj = docs[max_idx]
    score = scores[max_idx]["score"]
    corpus_id = scores[max_idx]["corpus_id"]
    return {
        "obj": obj,
        "score": score,
        "corpus_id": corpus_id
    }


@app.action("save_answer")
def save_answer(ack, body, say):
    ack()
    team = body["team"]["id"]
    user = body["user"]["id"]
    text = body["message"]["blocks"][1]["label"]["text"].split("Save Answer to ")[1]
    embedding = pickle.dumps(search_model.encode([text]))
    result = body["actions"][0]["value"]
    evidence = body["message"]["blocks"][0]["text"]["text"]
    query = schemas.QueryCreate(
        team=team,
        user=user,
        text=text,
        embedding=embedding,
        evidence=evidence,
        result=result
    )
    crud.create_query(db, query)
    say("Question and Answer added to knowledge base!")


@app.action("update_answer")
def update_answer(ack, body, say):
    ack()
    team = body["team"]["id"]
    text = body["message"]["blocks"][0]["label"]["text"].split("Save Answer to ")[1]
    query = crud.get_query_by_text(db, team, text)
    id = query.id
    update_fields = {"result": body["actions"][0]["value"]}
    crud.update_query(db, id, update_fields)
    say("Updated Answer!")


@app.action("override")
def verify_result(ack, body, say):
    ack()
    query = body["actions"][0]["value"]
    say(blocks=[
        {
            "dispatch_action": True,
            "type": "input",
            "element": {
                "type": "plain_text_input",
                "action_id": "update_answer"
            },
            "label": {
                "type": "plain_text",
                "text": f"Save Answer to {query}",
                "emoji": True
            }
        }
    ])

@app.event({
    "type": "message",
    "subtype": "message_deleted"
})
def handle_message_deleted(event, say):
    file_id = event["previous_message"]["files"][0]["id"]
    crud.delete_document(db, file_id)
    say("Document Deleted")


@app.event("message")
def handle_message(event, say):
    event_type = event.get("type")
    query = event.get("text")
    query_embedding = search_model.encode([query])
    if event_type == "message" and query is not None:
        team = event["team"]
        queries = crud.get_queries(db, team)
        most_similar_query = _get_most_similar_queries(queries, query_embedding)
        if most_similar_query is not None and most_similar_query["score"] >= 0.4:
            msq = most_similar_query["obj"]
            say(blocks=[
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": msq.evidence
			        }
		        },
                {
                    "type": "section",
                    "text": {
                        "type": "plain_text",
                        "text": f"Saved Answer: {msq.result}",
                        "emoji": True
                    }
		        },
                {
                    "type": "actions",
                    "elements": [
                        {
                            "type": "button",
                            "text": {
                                "type": "plain_text",
                                "text": "Override Answer"
                            },
                            "style": "danger",
                            "value": msq.text,
                            "action_id": "override"
                        }
                        
                    ]
				}
            ])
        else:
            documents = crud.get_documents(db, team)
            doc_obj = _get_most_similar_docs(documents, query_embedding)
            doc = doc_obj["obj"]
            corpus_id = doc_obj["corpus_id"]
            start, end = doc.word_positions.split("|")[corpus_id].split("_")
            private_url = doc.url
            name = doc.name
            text = _get_document_text(private_url, team)
            snippet = nlp(text)[int(start): int(end)].text
            blocks = [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f'{snippet} \n\n Source: <{private_url}|{name}>'
                    }
                },
                {
                    "dispatch_action": True,
                    "type": "input",
                    "element": {
                        "type": "plain_text_input",
                        "action_id": "save_answer"
                    },
                    "label": {
                        "type": "plain_text",
                        "text": f"Save Answer to {query}",
                        "emoji": True
                    }
                }
            ]
            say(blocks=blocks)


@app.command("/hashy")
def repeat_text(ack, respond, command):
    ack()
    if command.get("text") == "help":
        respond({
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": ":question: *Why create create text snippets when using Hashy?*\n:point_right: Hashy searches against your team's text snippets "
                        "and finds the most relevant section. The search result can be thought of as evidence for the provided answer or interpretation."
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": ":question: *Why provide answers when using Hashy?*\n:point_right: You can store answers to questions to provide additional context beyond return search result and provide a consistent "
                        "answer to similar future queries by you or your team."
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": ":question: *How do I create a text snippet in Slack?*\n:point_right: Checkout this <https://slack.com/help/articles/204145658-Create-or-paste-code-snippets-in-Slack|documentation> straight from the source."
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": ":question: *I can't view the source text snippet returned by the result?*\n:point_right: The creator of the snippet needs to directly share it with you or more ideally with a public channel such as `#hashy-snippets`."
                    }
                },
            ]
        })
    else:
        respond("Sorry, command not recognized")


@app.event("app_home_opened")
def handle_app_home_opened(client, event, say):
    user_id = event["user"]
    logged_user = crud.get_logged_user(db, user_id)
    if logged_user is None:
        result = client.users_info(
            user=user_id
        )
        crud.create_logged_user(
            db, schemas.LoggedUserCreate(user_id=user_id)
        ) 
        say(f"Hi, <@{result['user']['name']}>  :wave:\n\n"
            "I'm here to help you find and share knowledge across your organization!\n\n"
            ":page_facing_up: To get started create a text snippet from the shortcut menu, paste in your contents, and share with Hashy\n\n"
            ":mag: Search against your documents by DM'ing me or type in @Hashy followed by your query in other channels or conversations\n\n"
            ":lower_left_fountain_pen: Write down the answer to the query so your future self and team will get consistent answers to important questions\n\n"
            ":question: Type in `/hashy-help`\n\n"
            ":bulb: I recommend creating a public channel like `#hashy-snippets` to share your snippets with so your team members can view them"
        )

api = FastAPI()


@api.post("/slack/events")
async def endpoint(req: Request):
    return await app_handler.handle(req)


@api.get("/slack/install")
async def install(req: Request):
    return await app_handler.handle(req)


@api.get("/slack/oauth_redirect")
async def oauth_redirect(req: Request):
    return await app_handler.handle(req)
