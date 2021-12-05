import io
import itertools
import json
import logging
import pickle
import os
import re

import boto3
from fastapi import FastAPI, Request
import openai
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

openai.api_key = os.getenv("OPENAI_API_KEY")
sqs = boto3.resource("sqs", region_name="us-east-1")
queue = sqs.get_queue_by_name(QueueName=os.getenv("SQS_QUEUE_NAME"))


@app.event("file_created")
def handle_file_created_events(event, say):
    pass


@app.event("file_deleted")
def handle_file_deleted_events(body, logger):
    pass


@app.event("file_shared")
def handle_file_shared_events(body, logger):
    pass


@app.event("file_change")
def handle_file_created_events(client, event, say):
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


@app.event({
    "type": "message",
    "subtype": "file_share"
})
def handle_message_file_share(event, say):
    channel = event["channel"]
    for file in event["files"]:
        mimetype = file["mimetype"]
        filetype = file["filetype"]
        file_id = file["id"]
        user = file["user"]
        converted_pdf = file.get("converted_pdf")
        url = file["url_private"] if converted_pdf is None else converted_pdf
        file_name = file["name"]
        team = url.split("/")[4].split("-")[0]
        message = {
            "channel": channel,
            "mimetype": mimetype,
            "filetype": filetype,
            "file_id": file_id,
            "user": user,
            "converted_pdf": converted_pdf,
            "url": url,
            "file_name": file_name,
            "team": team
        }
        logger.info(message)
        say(f"Processing File {file_name}")
        queue.send_message(MessageBody=json.dumps(message))


def _get_most_similar_query(queries, embedding):
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


def _get_most_similar_doc(docs, embedding):
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


def _get_k_most_similar_docs(docs, embedding, k=3):
    scores = list(itertools.chain(*[
        util.semantic_search(embedding, pickle.loads(obj.embeddings), top_k=k)[0] for obj in docs
    ]))
    sorted_idx = sorted(range(len(scores)), key=lambda x: scores[x]["score"], reverse=True)
    snippets = []
    for idx in sorted_idx[:k]:
        doc_idx = idx // k
        doc = docs[doc_idx]
        name = doc.name
        private_url = doc.url
        team = doc.team
        if name.endswith(".pdf") or name.endswith(".docx"):
            text = _get_pdf_document_text(private_url, team)
        else:
            text = _get_txt_document_text(private_url, team)

        sentences = re.split(REGEX_EXP, text)
        corpus_id = scores[idx]["corpus_id"]
        if len(sentences) == 0:
            snippet = sentences[corpus_id]
        else:
            snippet = " ".join(sentences[corpus_id-1:corpus_id+2])
        text = f'{snippet} \n\n Source: <{private_url}|{name}>'
        snippets.append(text)
    return snippets


@app.action("save_answer")
def save_answer(ack, body, say):
    ack()
    team = body["team"]["id"]
    user = body["user"]["id"]
    text = [block["text"]["text"].split("Query: ")[1] for block in body["message"]["blocks"] if "Query:" in block.get("text", {}).get("text", "")][0]
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
        

@app.action("view_more_results")
def view_more_results(ack, body, client):
    ack()
    query = body["actions"][0]["value"]
    team = body["team"]["id"]
    embedding = search_model.encode([query])
    documents = crud.get_documents(db, team)
    snippets = _get_k_most_similar_docs(documents, embedding)
    blocks = []
    for idx, snippet in enumerate(snippets):
        if idx != 0:
            blocks.append({"type": "divider"})
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": snippet
            }
        })
    client.views_open(
        trigger_id=body["trigger_id"],
        view={
            "type": "modal",
            "title": {
                "type": "plain_text",
                "text": "Additional Results",
                "emoji": True
            },
            "blocks": blocks
        }
    )

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
    most_similar_query = _get_most_similar_query(queries, query_embedding)
    if most_similar_query is not None and most_similar_query["score"] >= 0.4:
        msq = most_similar_query["obj"]
        last_modified = msq.time_updated if msq.time_updated else msq.time_created
        source = msq.evidence.split("Source: ")[1] if "Source" in msq.evidence else None
        bot = installation_store.find_bot(
            enterprise_id=None,
            team_id=team,
        )
        client = WebClient(token=bot.bot_token)
        result = client.users_info(
            user=user
        )
        blocks = [
            {
                "type": "section",
                "text": {
                    "type": "plain_text",
                    "text": f"Saved Answer: {msq.result}",
                    "emoji": True
                }
            }
        ]
        if source is not None:
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"Source: {source}"
                }
            })
            blocks.append({
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {
                            "type": "plain_text",
                            "text": "View More Results",
                            "emoji": True
                        },
                        "value": query,
                        "action_id": "view_more_results"
                    }
                ]
		    })
        blocks.append({
            "type": "section",
            "text": {
                "type": "plain_text",
                "text": f"Last Updated by @{result['user']['name']} on {last_modified.month}/{last_modified.day}/{last_modified.year}",
                "emoji": True
            }
        })
        blocks.append({
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
        })
        say(blocks=blocks)
    else:
        documents = crud.get_documents(db, team)
        doc_obj = _get_most_similar_doc(documents, query_embedding)
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
                snippet = " ".join(sentences[corpus_id:corpus_id+2])
            snippet_processed = " ".join(snippet.split("\n")).strip()
            response = openai.Completion.create(
                engine="curie",
                prompt=f"Original: The Company and the Founders will provide the Investors with customary representations and warranties examples of which are set out in Appendix 4 and the Founders will provide the Investors with customary non-competition, non-solicitation and confidentiality undertakings.\nSummary: The Company and its Founders will provide the usual assurances and guarantees on facts about the business. The founders will also agree not to work for competitors, poach employees or customers when they leave the startup, and respect confidentiality.\n###\nOriginal: One immediately obvious and enormous area for Bitcoin-based innovation is international remittance. Every day, hundreds of millions of low-income people go to work in hard jobs in foreign countries to make money to send back to their families in their home countries – over $400 billion in total annually, according to the World Bank.\nSummary: Bitcoin can be an innovation for sending money overseas. The market opportunity is large. Workers send over $400 billion annually to their families in their home countries. \n###\nOriginal: In the event of an initial public offering of the Company's shares on a US stock exchange the Investors shall be entitled to registration rights customary in transactions of this type (including two demand rights and unlimited shelf and piggy-back rights), with the expenses paid by the Company.\nSummary: If the Company does an IPO in the USA, investors have the usual rights to include their shares in the public offering and the costs of d doing this will be covered by the Company.\n###\nOriginal: Finally, a fourth interesting use case is public payments. This idea first came to my attention in a news article a few months ago. A random spectator at a televised sports event held up a placard with a QR code and the text “Send me Bitcoin!” He received $25,000 in Bitcoin in the first 24 hours, all from people he had never met. This was the first time in history that you could see someone holding up a sign, in person or on TV or in a photo, and then send them money with two clicks on your smartphone: take the photo of the QR code on the sign, and click to send the money.\nSummary: Public payments is an interesting use case for Bitcoin. A person collected $25,000 in Bitcoin from strangers after holding up a QR code. It was the first time in history such an event occured.\n###\nOriginal: {snippet_processed}\n",
                temperature=0,
                max_tokens=32,
                top_p=1,
                frequency_penalty=1,
                presence_penalty=0,
                stop=["\n"]
            )
            snippet = response["choices"][0]["text"].split("Summary: ")[1]
            snippet = ".".join(snippet.split(".")[:-1])
            blocks = [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f'{snippet} \n\n Source: <{private_url}|{name}>'
                    }
                },
                {
                    "type": "actions",
                    "elements": [
                        {
                            "type": "button",
                            "text": {
                                "type": "plain_text",
                                "text": "View More Results",
                                "emoji": True
                            },
                            "value": query,
                            "action_id": "view_more_results"
                        }
                    ]
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
