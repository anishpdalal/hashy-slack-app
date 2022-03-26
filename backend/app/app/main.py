from asyncio import log
import base64
import itertools
import json
import logging
import os

import boto3
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from google_auth_oauthlib.flow import Flow
import requests
from slack_bolt import App
from slack_bolt.adapter.fastapi import SlackRequestHandler
from slack_bolt.oauth.oauth_settings import OAuthSettings
from slack_sdk.oauth.installation_store.sqlalchemy import SQLAlchemyInstallationStore
from slack_sdk.web import WebClient
from slack_sdk.errors import SlackApiError

from app.db import crud, database, schemas

logging.basicConfig(level=logging.INFO)
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


@app.event({
    "type": "message",
    "subtype": "message_deleted"
})
def handle_message_deleted(event, say):
    pass


@app.event({
    "type": "message",
})
def handle_message_channel(event, say, client):
    if event.get("channel_type") == "channel":
        channel = event["channel"]
        ts = event["ts"]
        query = event["text"]
        team = event["team"]
        user = event["user"]
        response = requests.post(
            f"{os.environ['API_URL']}/search",
            data=json.dumps({"team": team, "query": query, "user": user, "count": 10, "type": "channel"})
        ).json()
        modified_query = response["query"]
        search_scores = [res["score"] for res in response["search_results"]]
        answer_scores = [res["score"] for res in response["answers"]]
        max_search_score = max(search_scores) if len(search_scores) else 0
        max_answer_score = max(answer_scores) if len(answer_scores) else 0
        if max(max_search_score, max_answer_score) >= 0.50:
            message = f"Found documents with relevant content. Search with `/hashy {modified_query}`."
            client.chat_postMessage(channel=channel, thread_ts=ts, text=message)


@app.event({
    "type": "message",
    "subtype": "file_share"
})
def handle_message_file_share(event, say):
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


@app.view("submit_answer_view")
def handle_submit_answer_view(ack, body, client, view):
    ack()
    user = body["user"]["id"]
    user_name = body["user"]["name"]
    team = body["team"]["id"]
    text = [block["text"]["text"].split("Query: ")[1] for block in body["view"]["blocks"] if "Query:" in block.get("text", {}).get("text", "")][0]
    answer = view["state"]["values"]["save_answer"]["save_answer"]["value"]
    selected_conversations = view["state"]["values"]["ask_teammate"]["ask_teammate"]["selected_conversations"]
    if answer:
        query = {
            "team": team,
            "user": user,
            "text": text,
            "result": answer
        }
        requests.post(
            f"{os.environ['API_URL']}/create-answer",
            data=json.dumps(query)
        )
        client.chat_postMessage(
            channel=user,
            text="Successfully contributed an answer!"
        )
    db = database.SessionLocal()
    for id in selected_conversations:
        logged_user = crud.get_logged_user(db, id)
        if logged_user:
            msg = f"{user_name} has requested your help. Please enter `/hashy {text}` and provide an answer to the question. Thanks!"
        else:
            msg = f"{user_name} has requested your help. Please install the Hashy Slack app and enter `/hashy {text}` to provide an answer to the question. Thanks!"
        try:
            client.chat_postMessage(channel=id, text=msg)
        except SlackApiError as e:
            if e.response["error"] == "not_in_channel":
                channel_name = client.conversations_info(channel=id)["channel"]["name"]
                msg = f"The channel {channel_name} couldn't receive your question. Invite Hashy to the channel and ask again!"
                client.chat_postMessage(channel=user, text=msg)
    db.close()


@app.view("delete_answer_view")
def handle_delete_answer_view(ack, body, client, view):
    ack()
    query_ids = [opt["value"] for opt in view["state"]["values"]["delete_answers"]["delete_answers"]["selected_options"]]
    team = body["team"]["id"]
    user = body["user"]["id"]
    requests.post(
        f"{os.environ['API_URL']}/delete-answers",
        data=json.dumps({"team": team, "user": user, "query_ids": query_ids})
    )
    msg = "answers" if len(query_ids) > 1 else "answer"
    client.chat_postMessage(
        channel=user,
        text=f"Successfully deleted {len(query_ids)} {msg}!"
    )


def parse_summary(summary):
    if type(summary) == list:
        results = []
        results.append(" | ".join(list(summary[0].keys())))
        values = [" | ".join([str(val) for val in res.values()]) for res in summary]
        results.extend(values)
        return "\n".join(results)[0:3000]
    else:
        return summary


@app.action("increment_count")
def handle_some_action(ack, body, logger, client):
    ack()
    query_id = body["actions"][0]["value"]
    db = database.SessionLocal()
    query = crud.get_query(db, query_id)
    if query:
        upvotes = query.upvotes or 0
        voters = query.voters or []
        if body["user"]["id"] not in voters:
            voters.append(body["user"]["id"])
            voters = list(set(voters))
            crud.update_query(db, query_id, {"upvotes": upvotes + 1, "voters": voters})
            db.commit()
            for block in body["view"]["blocks"]:
                if block["block_id"] == query_id:
                    block["accessory"]["text"]["text"] = f":arrow_up: ({upvotes + 1})"
            client.views_update(
                hash=body["view"]["hash"],
                view_id=body["view"]["id"],
                view={
                    "callback_id": body["view"]["callback_id"],
                    "type": body["view"]["type"],
                    "title": body["view"]["title"],
                    "blocks": body["view"]["blocks"],
                    "private_metadata": body["view"]["private_metadata"],
                    "submit": body["view"]["submit"],
                    "close": body["view"]["close"],
                    "clear_on_close": body["view"]["clear_on_close"]
                }
            )
    db.close()
        

def answer_query(event, query):
    team = event["team"]
    user = event["user"]
    logger.info(json.dumps({
        "user": user,
        "team": team,
        "query": query
    }))
    
    if "|" in query:
        response = requests.post(
            f"{os.environ['API_URL']}/tabular-search",
            data=json.dumps({"team": team, "query": query, "user": user, "count": 10})
        ).json()
    else:
        response = requests.post(
            f"{os.environ['API_URL']}/search",
            data=json.dumps({"team": team, "query": query, "user": user, "count": 10})
        ).json()

    blocks = []
    sources = list(set([res.get("source") for res in response["search_results"]]))
    query_ids = list(set([res["id"] for res in response["answers"]]))
    db = database.SessionLocal()
    doc_user_mapping = crud.get_documents(db, sources)
    query_upvote_mapping = crud.get_queries(db, query_ids)
    db.close()
    score = 20
    score += min(2, len(sources)) * 10
    score += min(2, len(query_ids)) * 20
    score += min(2, sum([val or 0 for val in query_upvote_mapping.values()])) * 5
    score = score if score != 20 else 0
    blocks.append({
        "type": "header",
        "text": {
            "type": "plain_text",
            "text": f"Knowledge Freshness Score: {score}",
            "emoji": True
        }
    })
    if len(query_ids) < 2:
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "plain_text",
                    "text": "Increase the score for this topic by contributing a team answer",
                    "emoji": True
                }
            }
        )
    if response.get("summary") and user in doc_user_mapping.get(response["search_results"][0].get("source"), []):
        if response["summary"] != "Unknown":
            blocks.append({
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"Answer",
                    "emoji": True
                }
            })
            blocks.append(
                {
                    "type": "section",
                    "text": {
                        "type": "plain_text",
                        "text": parse_summary(response["summary"]),
                        "emoji": True
                    }
                }
            )
            blocks.append({"type": "divider"})

    if len(response["answers"]) > 0:
        blocks.append({
			"type": "header",
			"text": {
				"type": "plain_text",
				"text": f"Team Answers",
				"emoji": True
			}
		})
        response["answers"].sort(key=lambda x: query_upvote_mapping.get(x["id"], 0) or 0, reverse=True)
    
    for idx, result in enumerate(response["answers"]):
        if idx != 0:
            blocks.append({"type": "divider"})
        source = result.get("source","")
        name = result.get("name")
        result_text = result["result"]
        question = result.get("text", "")
        last_modified = result.get("last_modified", "")
        source_text = f"<{source}|{name}>" if source else f"{name} on {last_modified}"
        upvotes = query_upvote_mapping.get(result['id']) or 0
        blocks.append(
            {
                "block_id": result["id"],
                "type": "section",
                "text": {
                    "type": "plain_text",
                    "text": f"{result_text}",
                    "emoji": True
                },
                "accessory": {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": f":arrow_up: ({upvotes})"
                    },
                    "value": result["id"],
                    "action_id": "increment_count"
                }
            }
        )
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"\n\n _Source_: {source_text}\n\n_Responding to_: {question}"
                }
            }
        )
    
    if len(response["search_results"]) > 0:
        blocks.append({
			"type": "header",
			"text": {
				"type": "plain_text",
				"text": f"Search Results",
				"emoji": True
			}
		})
    
    for idx, result in enumerate(response["search_results"]):
        source = result.get("source","")
        if user not in doc_user_mapping.get(source, []):
            continue
        if idx != 0:
            blocks.append({"type": "divider"})
        name = result.get("name")
        result_text = result["result"]
        last_modified = result.get("last_modified", "")
        source_text = f"<{source}|{name}>" if source else f"{name} on {last_modified}"
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "plain_text",
                    "text": f"{result_text}",
                    "emoji": True
                }
            }
        )
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"\n\n Source: {source_text}"
                }
            }
        )
    
    return blocks


@app.event("app_mention")
def handle_mentions(event, say):
    pass


@app.event("message")
def handle_message(event, say):
    pass

SCOPE = 'https://www.googleapis.com/auth/drive.file'


@app.view("integration_view")
def handle_view_events(ack, body, client, view):
    integration = view["state"]["values"]["target_integration"]["target_integration_select"]["selected_option"]["value"]
    integration, channel = integration.split("-")
    user = body["user"]["id"]
    team = body["team"]["id"]
    db = database.SessionLocal()
    if integration.startswith("notion"):
        notion_id = os.environ["NOTION_CLIENT_ID"]
        redirect_uri = os.environ["NOTION_REDIRECT_URI"]
        msg = f"<https://api.notion.com/v1/oauth/authorize?owner=user&client_id={notion_id}&redirect_uri={redirect_uri}&response_type=code&state={user}-{team}|Click Here to integrate with Notion>"
    else:
        google_redirect_uri = os.environ["GOOGLE_REDIRECT_URI"]
        google_client_id = os.environ["GOOGLE_CLIENT_ID"]
        token = crud.get_google_token(db, user)
        if not token or not token.encrypted_token:
            msg = f"<https://accounts.google.com/o/oauth2/v2/auth?scope=https://www.googleapis.com/auth/drive.file&access_type=offline&prompt=consent&include_granted_scopes=true&response_type=code&state={user}-{team}&redirect_uri={google_redirect_uri}&client_id={google_client_id}|Click Here to integrate with Google Drive>"
        else:
            msg = f"<https://accounts.google.com/o/oauth2/v2/auth?scope=https://www.googleapis.com/auth/drive.file&access_type=offline&include_granted_scopes=true&response_type=code&state={user}-{team}&redirect_uri={google_redirect_uri}&client_id={google_client_id}|Click Here to integrate with Google Drive>"
    db.close()
    ack()
    client.chat_postMessage(channel=channel, text=msg)


@app.command("/hashy")
def help_command(ack, respond, command, client):
    ack()
    command_text = command.get("text")
    channel = command["channel_id"]
    user = command["user_id"]
    team = command["team_id"]
    if command_text == "help":
        respond({
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"To setup an integration, enter `/hashy integrate`"
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"To search or contribute answer to query, enter `/hashy <your query here>`"
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"To delete an answer, enter `/hashy delete`"
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"<https://calendly.com/taherhassonjee/hashy-onboarding|Schedule an Onboarding Call>"
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "Have any questions or feeback? Email us at help@nlp-labs.com"
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"<https://www.loom.com/share/2b10557ffb194ec692e1d7e063412ca2|Hashy Overview>"
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"<https://www.loom.com/share/91548cc56bee43a6a0d21e1cc91a7dfa|GDrive Integration Walk Through>"
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"<https://www.loom.com/share/eae6f60ae427436aa721cda203e15976|Notion Integration Walk Through>"
                    }
                }
            ]
        })
    elif command_text == "integrate":
        blocks = [
            {
                "block_id": "target_integration",
                "type": "input",
                "element": {
                    "type": "static_select",
                    "placeholder": {
                        "type": "plain_text",
                        "text": "Select an integration",
                        "emoji": True
                    },
                    "action_id": "target_integration_select",
                    "options": [
                        {
                            "text": {
                                "type": "plain_text",
                                "text": "Notion",
                                "emoji": True
                            },
                            "value": f"notion-{channel}"
                        },
                        {
                            "text": {
                                "type": "plain_text",
                                "text": "Google Drive",
                                "emoji": True
                            },
                            "value": f"gdrive-{channel}"
                        }
                    ]
                },
                "label": {
                    "type": "plain_text",
                    "text": "Choose document source",
                    "emoji": True
                }
		    }
	    ]
        response = client.views_open(
            trigger_id=command["trigger_id"],
            view={
                "callback_id": "integration_view",
                "type": "modal",
                "title": {
                    "type": "plain_text",
                    "text": f"Integration",
                    "emoji": True
                },
                "blocks": blocks,
                "title": {
                    "type": "plain_text",
                    "text": "Integrations",
                    "emoji": True
                },
                "submit": {
                    "type": "plain_text",
                    "text": "Submit",
                    "emoji": True
                },
                "close": {
                    "type": "plain_text",
                    "text": "Cancel",
                    "emoji": True
                }
            }
        )
    elif command_text == "delete":
        response = requests.post(
            f"{os.environ['API_URL']}/list-answers",
            data=json.dumps({"team": team, "user": user})
        ).json()
        options = [
            {
                "text": {
                    "type": "plain_text",
                    "text": res["answer"][0:70],
                    "emoji": True
                },
                "value": res["query_id"]
            } for res in response
        ]
        if len(options) > 0:
            client.views_open(
                trigger_id=command["trigger_id"],
                view={
                    "callback_id": "delete_answer_view",
                    "type": "modal",
                    "blocks": [{
                        "block_id": "delete_answers",
                        "type": "input",
                        "element": {
                            "type": "multi_static_select",
                            "placeholder": {
                                "type": "plain_text",
                                "text": "Select answers",
                                "emoji": True
                            },
                            "options": options,
                            "action_id": "delete_answers",
                        },
                        "label": {
                            "type": "plain_text",
                            "text": "Select answers to delete",
                            "emoji": True
                        }
                    }],
                    "title": {
                        "type": "plain_text",
                        "text": "Delete Answers",
                        "emoji": True
                    },
                    "submit": {
                        "type": "plain_text",
                        "text": "Submit",
                        "emoji": True
                    },
                    "close": {
                        "type": "plain_text",
                        "text": "Cancel",
                        "emoji": True
                    }
                }
            )
        else:
            client.views_open(
                trigger_id=command["trigger_id"],
                view={
                    "type": "modal",
                    "blocks": [
                        {
                            "type": "section",
                            "text": {
                                "type": "plain_text",
                                "text": "You do not have any saved answers",
                                "emoji": True
                            }
		                }
                    ],
                    "title": {
                        "type": "plain_text",
                        "text": "Delete Answers",
                        "emoji": True
                    },
                    "clear_on_close": True
                }
            )
    else:
        event = {
            "team": team,
            "user": user,
            "channel": channel
        }
        blocks = [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"Query: {command_text}"
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"Loading Results..."
                }
            },

        ]
        response = client.views_open(
            trigger_id=command["trigger_id"],
            view={
                "type": "modal",
                "title": {
                    "type": "plain_text",
                    "text": f"Results",
                    "emoji": True
                },
                "blocks": blocks
            }
        )
        blocks = [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"Query: {command_text}"
                }
            }
        ]
        result_blocks = answer_query(event, command_text)
        blocks.extend(result_blocks)
        blocks.extend([
            {
                "block_id": "save_answer",
                "type": "input",
                "optional": True,
                "element": {
                    "type": "plain_text_input",
                    "action_id": "save_answer",
                    "multiline": True,
                },
                "label": {
                    "type": "plain_text",
                    "text": f"Have an answer you'd like to contribute? Save it here:",
                    "emoji": True
                }
            },
            {
                "block_id": "ask_teammate",
                "type": "input",
                "optional": True,
                "element": {
                    "type": "multi_conversations_select",
                    "placeholder": {
                        "type": "plain_text",
                        "text": "Select teammates and/or channels",
                        "emoji": True
                    },
                    "action_id": "ask_teammate",
                },
                "label": {
                    "type": "plain_text",
                    "text": "Think somebody else can provide a great answer? Ask teammates to contribute.",
                    "emoji": True
                }
            }
        ])
        client.views_update(
            hash=response["view"]["hash"],
            view_id=response["view"]["id"],
            view={
                "callback_id": "submit_answer_view",
                "type": "modal",
                "title": {
                    "type": "plain_text",
                    "text": "Results",
                    "emoji": True
                },
                "blocks": blocks,
                "private_metadata": channel,
                "submit": {
                    "type": "plain_text",
                    "text": "Submit",
                    "emoji": True
                },
                "close": {
                    "type": "plain_text",
                    "text": "Close",
                    "emoji": True
                },
                "clear_on_close": True
            }
        )


@app.event("app_home_opened")
def handle_app_home_opened(client, event, say):
    user_id = event["user"]
    db = database.SessionLocal()
    try:
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
            user = crud.create_logged_user(
                db, schemas.LoggedUserCreate(
                    user_id=user_id,
                    team_id=team_id,
                    team_name=team_name
                )
            )
            db.add(user)
            db.commit()
            db.refresh(user)
            say(f":wave: Hi <@{result['user']['name']}>, Welcome to the Hashy app on Slack! Hashy lets you quickly find and share knowledge across your org.\n"
                "Here's how to get started :point_down:\n\n"
                f"1. Think of a question you get asked all the time. Type in `/hashy <your question here>` and enter your answer\n"
                "Users tend to answer with written explanations or links to documents/videos/web pages.\n\n"
                f"2. Congrats! Your team can get your answer from Hashy rather than directly asking you all the time :smile:.\n"
                "The next time someone asks the same query, they'll see the answer you provided. Go ahead and try it out!\n\n"
                f"3. Now think of an important question that someone else knows how to answer. Type in `/hashy <your question here>`\n"
                "and select the coworker to ask the question to. The coworker will be prompted to enter an answer.\n"
                "This is how Hashy seamlessly captures knowledge across teams.\n\n"
                "4. To learn how to get the most out of Hashy, book a 15 min <https://calendly.com/taherhassonjee/hashy-onboarding|Onboarding Call> with us.\n\n"
                "5. Enter `/hashy help` to learn how to setup integrations and also watch an overview of Hashy.\n\n"
            )
    except:
        db.rollback()
        raise
    finally:
        db.close()

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


@api.get("/google/oauth_redirect")
async def google_authorize(req: Request, state):
    user_id, team_id = state.split("-")
    config = {
        "web":{
            "client_id": os.environ["GOOGLE_CLIENT_ID"],
            "project_id": os.environ["GOOGLE_PROJECT_ID"],
            "auth_uri":" https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "client_secret": os.environ["GOOGLE_SECRET_KEY"],
            "redirect_uris": [os.environ["GOOGLE_REDIRECT_URI"]],
            "javascript_origins": [req.url.components.netloc]
        }
    }
    flow = Flow.from_client_config(
      config, scopes='https://www.googleapis.com/auth/drive.file', state=state
    )
    flow.redirect_uri = os.environ["GOOGLE_REDIRECT_URI"]
    auth_response = req.url.components
    auth_response = f"https://{auth_response.netloc}{auth_response.path}?{auth_response.query}"
    flow.fetch_token(authorization_response=auth_response)
    credentials = flow.credentials
    db = database.SessionLocal()
    try:
        token = crud.get_google_token(db, user_id)
        if not token:
            fields = {
                "user_id": user_id,
                "team": team_id,
                "encrypted_token": credentials.refresh_token
            }
            token = crud.create_google_token(fields)
            db.add(token)
            db.commit()
            db.refresh(token)
        else:
            if credentials.refresh_token is not None:
                fields = {}
                fields["encrypted_token"] = credentials.refresh_token
                crud.update_google_token(db, token.id, fields)
                db.commit()
    except:
        db.rollback()
        raise
    finally:
        db.close()
    app_id = os.environ["GOOGLE_APP_ID"]
    key = os.environ["GOOGLE_API_KEY"]
    response = RedirectResponse(f"/google-picker/{credentials.token}?team={team_id}&user={user_id}&id={app_id}&key={key}")
    return response


@api.get("/google-picker/{token}")
async def google_picker(token, team, user, id, key):
    html = """
    <html xmlns="http://www.w3.org/1999/xhtml">
        <head>
            <meta charset="utf-8" />
            <title>Google Picker Example</title>

            <script type="text/javascript">

            var pickerApiLoaded = false;
            var oauthToken = window.location.pathname.split("/")[2];
            var urlParams = new URLSearchParams(window.location.search);
            var developerKey = urlParams.get("key");
            var team = urlParams.get("team");
            var user = urlParams.get("user");
            var appId = urlParams.get("id");

            function loadPicker() {
                gapi.load('picker', {'callback': onPickerApiLoad});
            }


            function onPickerApiLoad() {
                pickerApiLoaded = true;
                createPicker();
            }

            function createPicker() {
                if (pickerApiLoaded && oauthToken) {
                    var DisplayView = new google.picker.DocsView().
                        setMimeTypes("application/vnd.google-apps.document,application/pdf,application/vnd.google-apps.spreadsheet").
                        setMode(google.picker.DocsViewMode.LIST)
                    var ShareView = new google.picker.DocsView().
                        setMimeTypes("application/vnd.google-apps.document,application/pdf,application/vnd.google-apps.spreadsheet").
                        setMode(google.picker.DocsViewMode.LIST).
                        setEnableDrives(true)
                    var picker = new google.picker.PickerBuilder().
                        enableFeature(google.picker.Feature.MULTISELECT_ENABLED).
                        enableFeature(google.picker.Feature.SUPPORT_DRIVES).
                        addView(ShareView).
                        addView(DisplayView).
                        setAppId(appId).
                        setOAuthToken(oauthToken).
                        setDeveloperKey(developerKey).
                        setCallback(pickerCallback).
                        setTitle("Selected files to search").
                        build().
                        setVisible(true);
                }
            }

            function pickerCallback(data) {
                if (data.action == google.picker.Action.PICKED) {
                    var files = [];
                    for (let i = 0; i < data.docs.length; i++) {
                        files.push({
                            "file_id": data.docs[i].id,
                            "file_name": data.docs[i].name,
                            "mime_type": data.docs[i].mimeType
                        });
                    }
                    var xmlhttp = new XMLHttpRequest();
                    var theUrl = `https://${window.location.hostname}/google/process-documents`;
                    xmlhttp.open("POST", theUrl);
                    xmlhttp.setRequestHeader("Content-Type", "application/json");
                    xmlhttp.send(JSON.stringify({"team": team, "user": user, "token": oauthToken, files: files}));
                    loadPicker();
                }
            }
            </script>
        </head>
        <body>
            <div style="width:15%; margin-top: 10%;">
                <p style="color:black;"><b>Current Files formats supported: PDF, Google Doc, Google Sheet</b></p>
                <p style="color:black;"><b>Select All Files: Shift + a</b></p>
                <p style="color:black;"><b>Clear All Selections: Shift + n</b></p>
                <p style="color:black;"><b>Select/Unselect Individual Files: Hold Ctl/Cmd + click</b></p>
            </div>
            <div id="result"></div>
            <!-- The Google API Loader script. -->
            <script type="text/javascript" src="https://apis.google.com/js/api.js?onload=loadPicker"></script>
        </body>
    </html>
    """
    return HTMLResponse(html)


def chunks(iterable, batch_size=10):
    """A helper function to break an iterable into chunks of size batch_size."""
    it = iter(iterable)
    chunk = tuple(itertools.islice(it, batch_size))
    while chunk:
        yield chunk
        chunk = tuple(itertools.islice(it, batch_size))


@api.post("/google/process-documents")
def process_google_documents(upload: schemas.GooglePickerUpload):
    user_id = upload.user
    team_id = upload.team
    files_to_upload = [
        {
            "Id": file["file_id"],
            "MessageBody": json.dumps(
                {
                    "team": team_id,
                    "user": user_id,
                    "url": f"https://drive.google.com/file/d/{file['file_id']}",
                    "filetype": f"drive#file|{file['mime_type']}",
                    "file_name": file["file_name"],
                    "file_id": file["file_id"]
                }
            )
        } for file in upload.files
    ]
    for files_chunk in chunks(files_to_upload, batch_size=10):
        queue.send_messages(Entries=files_chunk)
    bot = installation_store.find_bot(
        enterprise_id=None,
        team_id=team_id,
    )
    client = WebClient(token=bot.bot_token)
    client.chat_postMessage(
        channel=user_id,
        text="Integrating google drive documents"
    )


@api.get("/notion/oauth_redirect")
async def notion_oauth_redirect(code, state):
    credential = f"{os.environ['NOTION_CLIENT_ID']}:{os.environ['NOTION_SECRET_KEY']}"
    credential_bytes = credential.encode("ascii")
    base64_bytes = base64.b64encode(credential_bytes)
    base64_credential = base64_bytes.decode('ascii')
    token_response = requests.post(
        "https://api.notion.com/v1/oauth/token",
        headers={
            "Authorization": f"Basic {base64_credential}",
            "Content-type": "application/json"
        },
        data=json.dumps(
            {
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": os.environ["NOTION_REDIRECT_URI"]
            }
        )
    )
    token_response = token_response.json()
    db = database.SessionLocal()
    try:
        user_id, team_id = state.split("-")
        notion_user_id = token_response["owner"]["user"]["id"]
        access_token = token_response["access_token"]
        bot_id = token_response["bot_id"]
        workspace_id = token_response["workspace_id"]
        fields = {
            "user_id": user_id,
            "team": team_id,
            "notion_user_id": notion_user_id,
            "encrypted_token": access_token,
            "bot_id": bot_id,
            "workspace_id": workspace_id
        }
        token = crud.get_notion_token(db, user_id)
        if token:
            crud.update_notion_token(db, token.id, fields)
            db.commit()
        else:
            token = crud.create_notion_token(schemas.NotionTokenCreate(**fields))
            db.add(token)
            db.commit()
            db.refresh(token)
    except:
        db.rollback()
        raise
    finally:
        db.close()
    
    lambda_client = boto3.client("lambda", region_name="us-east-1")
    payload = {"team": team_id, "user": user_id}
    lambda_client.invoke(
        FunctionName=os.environ["LAMBDA_FUNCTION"],
        InvocationType="Event",
        Payload=json.dumps(payload)
    )
    bot = installation_store.find_bot(
        enterprise_id=None,
        team_id=team_id,
    )
    client = WebClient(token=bot.bot_token)
    client.chat_postMessage(
        channel=user_id,
        text="Integrating notion documents. It may take up to a few hours to complete processing."
    )
    response = RedirectResponse(f"https://app.slack.com/client/{team_id}")
    return response
