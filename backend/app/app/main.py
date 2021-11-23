import io
import json
import logging
import pickle
import os
import re

from fastapi import FastAPI, Request
import pdfminer.high_level
from slack_bolt import App
from slack_bolt.adapter.fastapi import SlackRequestHandler
from slack_bolt.oauth.oauth_settings import OAuthSettings
from slack_sdk.web import WebClient
from slack_sdk.oauth.installation_store.sqlalchemy import SQLAlchemyInstallationStore
import requests
from sentence_transformers import SentenceTransformer, util

from app.db import crud, database, schemas

logging.basicConfig(level=logging.INFO)
logging.getLogger("pdfminer").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


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

REGEX_EXP = r"(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|\?)\s"
search_model = SentenceTransformer('msmarco-distilbert-base-v4')
db = database.SessionLocal()



@app.event("file_created")
def handle_file_created_events(event, say):
    pass


@app.event("file_deleted")
def handle_file_deleted_events(body, logger):
    pass


@app.event("file_shared")
def handle_file_shared_events(body, logger):
    pass


def _get_txt_document_text(url, team):
    bot = installation_store.find_bot(
        enterprise_id=None,
        team_id=team,
    )
    headers = {
        "Authorization": f"Bearer {bot.bot_token}",
        "Content-Type": "text/html"
    }
    text = requests.get(url, headers=headers).text
    return text


def _process_plain_text(file):
    url_private = file["url_private"]
    team = url_private.split("/")[4].split("-")[0]
    name = file["name"]
    text = _get_txt_document_text(url_private, team)
    sentences = re.split(REGEX_EXP, text)
    doc_embeddings = search_model.encode(sentences)
    doc = schemas.DocumentCreate(
        team=team,
        name=name,
        url=url_private,
        embeddings=pickle.dumps(doc_embeddings),
        file_id=file["id"],
        user=file["user"]
    )
    crud.create_document(db, doc)


def _get_pdf_document_text(url, team):
    bot = installation_store.find_bot(
        enterprise_id=None,
        team_id=team,
    )
    headers = {
        "Authorization": f"Bearer {bot.bot_token}",
    }
    byte_str = requests.get(url, headers=headers).content
    pdf_memory_file = io.BytesIO()
    pdf_memory_file.write(byte_str)
    text = pdfminer.high_level.extract_text(pdf_memory_file)
    return text


def _process_pdf(file):
    private_url = file["url_private"]
    team = private_url.split("/")[4].split("-")[0]
    name = file["name"]
    text = _get_pdf_document_text(private_url, team)
    sentences = re.split(REGEX_EXP, text)
    doc_embeddings = search_model.encode(sentences)
    doc = schemas.DocumentCreate(
        team=team,
        name=name,
        url=private_url,
        embeddings=pickle.dumps(doc_embeddings),
        file_id=file["id"],
        user=file["user"]
    )
    crud.create_document(db, doc)


@app.event({
    "type": "message",
    "subtype": "file_share"
})
def handle_message_file_share(event, say):
    file = event["files"][0]
    mimetype = file["mimetype"]
    filetype = file["filetype"]
    say(f"Processing File {file['name']}")
    if mimetype == "text/plain":
        _process_plain_text(file)
    elif mimetype == "application/pdf":
        _process_pdf(file)
    elif filetype == "docx" and "converted_pdf" in file:
        file["url_private"] = file["converted_pdf"]
        _process_pdf(file)
    say(f"Finished processing File {file['name']}")


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
    text = body["message"]["blocks"][1]["text"]["text"].split("Query: ")[1]
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
    text = body["message"]["blocks"][0]["text"]["text"].split("Query: ")[1]
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
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"Query: {query}"
            }
        },
        {
            "dispatch_action": True,
            "type": "input",
            "element": {
                "type": "plain_text_input",
                "action_id": "update_answer"
            },
            "label": {
                "type": "plain_text",
                "text": "Have a way to improve the answer? Save it here:",
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
    doc = crud.get_document(db, file_id)
    if doc is not None:
        crud.delete_document(db, file_id)
        

def process_query(event, say, query):
    query_embedding = search_model.encode([query])
    team = event["team"]
    user = event["user"]
    logger.info(json.dumps({
        "user": user,
        "team": team,
        "query": query
    }))
    queries = crud.get_queries(db, team)
    most_similar_query = _get_most_similar_queries(queries, query_embedding)
    if most_similar_query is not None and most_similar_query["score"] >= 0.4:
        msq = most_similar_query["obj"]
        last_modified = msq.time_updated if msq.time_updated else msq.time_created
        source = msq.evidence.split("Source: ")[1]
        bot = installation_store.find_bot(
            enterprise_id=None,
            team_id=team,
        )
        client = WebClient(token=bot.bot_token)
        result = client.users_info(
            user=user
        )
        say(blocks=[
            {
                "type": "section",
                "text": {
                    "type": "plain_text",
                    "text": f"Saved Answer: {msq.result}",
                    "emoji": True
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"Source: {source}"
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "plain_text",
                    "text": f"Last Updated by @{result['user']['name']} on {last_modified.month}/{last_modified.day}/{last_modified.year}",
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
        if doc_obj is None:
            blocks = [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "No matching document found"
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"Query: {query}"
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
                        "text": f"Have an answer? Save it here:",
                        "emoji": True
                    }
                }
            ]
            say(blocks=blocks)
            return
        doc = doc_obj["obj"]
        corpus_id = doc_obj["corpus_id"]
        score = doc_obj["score"]
        if score >= 0.3:
            private_url = doc.url
            name = doc.name
            if name.endswith(".pdf") or name.endswith(".docx"):
                text = _get_pdf_document_text(private_url, team)
            else:
                text = _get_txt_document_text(private_url, team)
            sentences = re.split(REGEX_EXP, text)
            if len(sentences) == 0:
                snippet = sentences[corpus_id]
            else:
                snippet = " ".join(sentences[corpus_id-1:corpus_id+2])
            blocks = [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f'{snippet} \n\n Source: <{private_url}|{name}>'
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"Query: {query}"
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
                        "text": f"Have a better answer? Save it here:",
                        "emoji": True
                    }
                }
            ]
            say(blocks=blocks)
        else:
            blocks = [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "No matching document found"
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"Query: {query}"
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
                        "text": f"Have an answer? Save it here:",
                        "emoji": True
                    }
                }
            ]
            say(blocks=blocks)
            


@app.event("app_mention")
def handle_mentions(event, say):
    query = event["text"].split("> ")[1]
    if query is not None:
        process_query(event, say, query)


@app.event("message")
def handle_message(event, say):
    channel_type = event.get("channel_type")
    query = event.get("text")
    if query is not None and channel_type == "im":
        process_query(event, say, query)


@app.event("file_change")
def handle_file_created_events(client, event, say):
    file_id = event["file_id"]
    document = crud.get_document(db, file_id)
    if document is not None:
        file = client.files_info(
            file=file_id
        )
        file_name = file["file"]["name"]
        private_url = file["file"]["url_private"]
        team = document.team
        text = _get_txt_document_text(private_url, team)
        sentences = re.split(REGEX_EXP, text)
        doc_embeddings = search_model.encode(sentences)
        embeddings = pickle.dumps(doc_embeddings)
        update_fields = {
            "embeddings": embeddings,
            "name": file_name,
            "url": private_url,
        }
        crud.update_document(db, document.id, update_fields)
        logger.info(f"{file_name} updated")


@app.command("/hashy")
def repeat_text(ack, respond, command):
    ack()
    command_text = command.get("text")
    if command_text == "help":
        respond({
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "Checkout this <https://www.loom.com/share/80845208ecd343e2a5efddb2158ae69d|demo> for a more detailed walked-through and explanation of Hashy"
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": ":question: *How do I use Hashy in a public channel?*\n:point_right: Mention `@Hashy` followed by your query."
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": ":question: *How do I create a text snippet in Slack?*\n:point_right: Checkout this <https://slack.com/help/articles/204145658-Create-or-paste-code-snippets-in-Slack|documentation> straight from the source. "
                        "Alternatively you can upload a plain text file when you are DMing Hashy."
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
        event = {"team": command["team_id"], "user": command["user_id"]}
        process_query(event, respond, command_text)


@app.event("app_home_opened")
def handle_app_home_opened(client, event, say):
    user_id = event["user"]
    logged_user = crud.get_logged_user(db, user_id)
    if logged_user is None:
        result = client.users_info(
            user=user_id
        )
        team_id = result["user"]["team_id"]
        team_info = client.team_info(
            team=team_id
        )
        team_name = team_info["team"]["name"]
        crud.create_logged_user(
            db, schemas.LoggedUserCreate(
                user_id=user_id,
                team_id=team_id,
                team_name=team_name
            )
        ) 
        say(f"Hi, <@{result['user']['name']}>  :wave:\n\n"
            "I'm here to help you find and share knowledge across your organization. Let's get started with an example!\n\n"
            "1. Type `/` to pull up the shortcut menu and search for `Create a text snippet`. Title the snippet `Sales Agreement` and add the following text to the Content Box: `Company XYZ bought 100 units in October 2021`\n\n"
            "2. Message Hashy the following: How many units did Company XYZ purchase?\n\n"
            "3. Congrats! You now know what you need to use Hashy. Want to help your team further? Provide your own answer or interpretation\n\n"
            "Checkout this <https://www.loom.com/share/80845208ecd343e2a5efddb2158ae69d|demo> for a more detailed walked-through and explanation of Hashy\n\n"
            "Type in `/hashy help` to get more information about using Hashy.\n\n"
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
