import base64
import json
import logging
import os

import boto3
from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse
import requests
from slack_bolt import App
from slack_bolt.adapter.fastapi import SlackRequestHandler
from slack_bolt.oauth.oauth_settings import OAuthSettings
from slack_sdk.oauth.installation_store.sqlalchemy import SQLAlchemyInstallationStore
from slack_sdk.web import WebClient

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


@app.action("save_answer")
def save_answer(ack, body, client):
    ack()
    view_id = body["container"]["view_id"]
    team = body["team"]["id"]
    user = body["user"]["id"]
    text = [block["text"]["text"].split("Query: ")[1] for block in body["view"]["blocks"] if "Query:" in block.get("text", {}).get("text", "")][0]
    result = body["actions"][0]["value"]
    query = {
        "team": team,
        "user": user,
        "text": text,
        "result": result
    }
    requests.post(
        f"{os.environ['API_URL']}/create-answer",
        data=json.dumps(query)
    )
    blocks = [{
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": f"Query: {text}"
        }
    }]
    event = {
        "team": team,
        "user": user
    }
    result_blocks = answer_query(event, text)
    blocks.extend(result_blocks)
    blocks.append({
        "dispatch_action": True,
        "type": "input",
        "element": {
            "type": "plain_text_input",
            "action_id": "save_answer"
        },
        "label": {
            "type": "plain_text",
            "text": f"Have an answer you'd like to contribute? Save it here:",
            "emoji": True
        }
    })
    client.views_update(
        view_id=view_id,
        view={
            "type": "modal",
            "title": {
                "type": "plain_text",
                "text": "Results",
                "emoji": True
            },
            "blocks": blocks
        }
    )


@app.event({
    "type": "message",
    "subtype": "message_deleted"
})
def handle_message_deleted(event, say):
    file_id = event["previous_message"]["files"][0]["id"]
    db = database.SessionLocal()
    try:
        doc = crud.get_document(db, file_id)
        if doc is not None:
            crud.delete_document(db, file_id)
        db.commit()
    except:
        db.rollback()
        raise
    finally:
        db.close()
        

def answer_query(event, query):
    team = event["team"]
    user = event["user"]
    logger.info(json.dumps({
        "user": user,
        "team": team,
        "query": query
    }))
    response = requests.post(
        f"{os.environ['API_URL']}/search",
        data=json.dumps({"team": team, "query": query, "user": user, "count": 10})
    ).json()

    blocks = []
    if len(response["answers"]) > 0:
        blocks.append({
			"type": "header",
			"text": {
				"type": "plain_text",
				"text": f"Team Answers ({len(response['answers'])})",
				"emoji": True
			}
		})
    
    for idx, result in enumerate(response["answers"]):
        if idx != 0:
            blocks.append({"type": "divider"})
        source = result.get("source","")
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
    
    if len(response["search_results"]) > 0:
        blocks.append({
			"type": "header",
			"text": {
				"type": "plain_text",
				"text": f"Search Results ({len(response['search_results'])})",
				"emoji": True
			}
		})
    

    for idx, result in enumerate(response["search_results"]):
        if idx != 0:
            blocks.append({"type": "divider"})
        source = result.get("source","")
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
    query = event["text"].split("> ")[1]
    if query is not None:
        say(f"Paste in `/hashy {query}`")


@app.event("message")
def handle_message(event, say):
    channel_type = event.get("channel_type")
    query = event.get("text")
    if query is not None and channel_type == "im":
        say(f"Paste in `/hashy {query}`")


@app.command("/hashy")
def help_command(ack, respond, command, client):
    ack()
    command_text = command.get("text")
    channel = command["channel_id"]
    user = command["user_id"]
    team = command["team_id"]
    notion_id = os.environ["NOTION_CLIENT_ID"]
    redirect_uri = os.environ["NOTION_REDIRECT_URI"]
    if command_text == "help":
        respond({
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"Integrate with <https://api.notion.com/v1/oauth/authorize?owner=user&client_id={notion_id}&redirect_uri={redirect_uri}&response_type=code&state={user}-{team}-{channel}|Notion>. Takes up to 1 hour to process all documents."
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"To search enter `/hashy <your query here>`"
                    }
                }
            ]
        })
    else:
        event = {
            "team": team,
            "user": user
        }
        blocks = [{
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"Query: {command_text}"
            }
        }]
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
        result_blocks = answer_query(event, command_text)
        blocks.extend(result_blocks)
        blocks.append({
            "dispatch_action": True,
            "type": "input",
            "element": {
                "type": "plain_text_input",
                "action_id": "save_answer"
            },
            "label": {
                "type": "plain_text",
                "text": f"Have an answer you'd like to contribute? Save it here:",
                "emoji": True
            }
        })
        client.views_update(
            hash=response["view"]["hash"],
            view_id=response["view"]["id"],
            view={
                "type": "modal",
                "title": {
                    "type": "plain_text",
                    "text": "Results",
                    "emoji": True
                },
                "blocks": blocks
            }
        )


@app.event("app_home_opened")
def handle_app_home_opened(client, event, say):
    user_id = event["user"]
    channel_id = event["channel"]
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
            notion_id = os.environ["NOTION_CLIENT_ID"]
            redirect_uri = os.environ["NOTION_REDIRECT_URI"]
            say(f"Hi, <@{result['user']['name']}>  :wave:\n\n"
                f"Integrate with <https://api.notion.com/v1/oauth/authorize?owner=user&client_id={notion_id}&redirect_uri={redirect_uri}&response_type=code&state={user_id}-{team_id}-{channel_id}|Notion>. Takes up to 1 hour to process all documents.\n\n"
                "To search enter `/hashy <your query here>`\n\n"
                "Type in `/hashy help` to pull up these instructions again\n\n"
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
        user_id, team_id, channel_id = state.split("-")
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
    
    request_body = {
        "sort": {
            "direction": "descending",
            "timestamp": "last_edited_time"
        },
        "filter": {
            "property": "object",
            "value": "page"
        }
    }

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-type": "application/json",
        "Notion-Version": "2021-08-16"
    }
    api_url = "https://api.notion.com/v1/search"
    search_results = []
    results = requests.post(api_url, headers=headers, data=json.dumps(request_body)).json()
    search_results.extend(results["results"])
    while results.get("has_more"):
        request_body["start_cursor"] = results["next_cursor"]
        results = requests.post(api_url, headers=headers, data=json.dumps(request_body)).json()
        search_results.extend(results["results"])

    logger.info(f"Processing {len(search_results)} Documents")
    for res in search_results:
        url = res["url"]
        split_url = url.split("/")[-1].split("-")
        if len(split_url) == 1:
            file_name = "Untitled"
        else:
            file_name = " ".join(split_url[:-1])
        page = {
            "team": team_id,
            "user": user_id,
            "url": url,
            "filetype": "notion",
            "file_name": file_name,
            "file_id": res["id"]
        }
        queue.send_message(MessageBody=json.dumps(page))

    bot = installation_store.find_bot(
        enterprise_id=None,
        team_id=team_id,
    )
    client = WebClient(token=bot.bot_token)
    client.chat_postMessage(
        channel=channel_id,
        text=f"Integrated with your Notion account! Start searching your documents as Hashy continues to stay in sync with your Notion account."
    )

    response = RedirectResponse(f"https://app.slack.com/client/{team_id}")
    return response
