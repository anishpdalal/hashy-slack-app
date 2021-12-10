import base64
import json
import logging
import os

import boto3
from fastapi import FastAPI, Request, Response
import requests
from slack_bolt import App
from slack_bolt.adapter.fastapi import SlackRequestHandler
from slack_bolt.oauth.oauth_settings import OAuthSettings
from slack_sdk.oauth.installation_store.sqlalchemy import SQLAlchemyInstallationStore

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
def save_answer(ack, body, say):
    ack()
    team = body["team"]["id"]
    user = body["user"]["id"]
    text = [block["text"]["text"].split("Query: ")[1] for block in body["message"]["blocks"] if "Query:" in block.get("text", {}).get("text", "")][0]
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
    say("Question and Answer added to knowledge base!")


@app.action("update_answer")
def update_answer(ack, body, say):
    ack()
    team = body["team"]["id"]
    text = body["message"]["blocks"][0]["text"]["text"].split("Query: ")[1]
    db = database.SessionLocal()
    try:
        query = crud.get_query_by_text(db, team, text)
        id = query.id
        update_fields = {"result": body["actions"][0]["value"]}
        crud.update_query(db, id, update_fields)
        db.commit()
    except:
        db.rollback()
        raise
    finally:
        db.close()
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
        

@app.action("view_more_results")
def view_more_results(ack, body, client):
    ack()
    query = body["actions"][0]["value"]
    team = body["team"]["id"]
    results = requests.post(
        f"{os.environ['API_URL']}/search",
        data=json.dumps({"team": team, "query": query, "count": 3})
    )
    results = json.loads(results.text)
    blocks = []
    for idx, result in enumerate(results):
        if idx != 0:
            blocks.append({"type": "divider"})
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"{result['result']}\n\n<{result['source']}|{result['name']}>"
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

def answer_query(event, say, query):
    team = event["team"]
    user = event["user"]
    logger.info(json.dumps({
        "user": user,
        "team": team,
        "query": query
    }))
    results = requests.post(
        f"{os.environ['API_URL']}/answer",
        data=json.dumps({"team": team, "query": query})
    )
    results = json.loads(results.text)
    if len(results) == 0:
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
    else:
        result = results[0]
        source = result.get("source","")
        name = result.get("name", result.get("user", ""))
        team = result["team"]
        text = result["text"]
        last_modified = result.get("last_modified", "")
        result_text = result["result"]
        source_text = f"<{source}|{name}>" if source else f"{name} on {last_modified}"
        blocks = [
            {
                "type": "section",
                "text": {
                    "type": "plain_text",
                    "text": f"{result_text}",
                    "emoji": True
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"\n\n Source: {source_text}"
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
		    }
        ]
        if result.get("user"):
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
                        "value": text,
                        "action_id": "override"
                    }
                ]
            })
        else:
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"Query: {query}"
                }
            })
            blocks.append({
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
            })
        say(blocks=blocks)


@app.event("app_mention")
def handle_mentions(event, say):
    query = event["text"].split("> ")[1]
    if query is not None:
        answer_query(event, say, query)


@app.event("message")
def handle_message(event, say):
    channel_type = event.get("channel_type")
    query = event.get("text")
    if query is not None and channel_type == "im":
        answer_query(event, say, query)


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
    elif command_text == "notion":
        user = command["user_id"]
        team = command["team_id"]
        notion_id = os.environ["NOTION_CLIENT_ID"]
        redirect_uri = os.environ["NOTION_REDIRECT_URI"]
        respond({
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"<https://api.notion.com/v1/oauth/authorize?owner=user&client_id={notion_id}&redirect_uri={redirect_uri}&response_type=code&state={user}-{team}|Add Notion>"
                    }
                }
            ]
        })


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
            say(f"Hi, <@{result['user']['name']}>  :wave:\n\n"
                "I'm here to help you find and share knowledge across your organization. Let's get started with an example!\n\n"
                "1. Type `/` to pull up the shortcut menu and search for `Create a text snippet`. Title the snippet `Sales Agreement` and add the following text to the Content Box: `Company XYZ bought 100 units in October 2021`\n\n"
                "2. Message Hashy the following: How many units did Company XYZ purchase?\n\n"
                "3. Congrats! You now know what you need to use Hashy. Want to help your team further? Provide your own answer or interpretation\n\n"
                "Checkout this <https://www.loom.com/share/80845208ecd343e2a5efddb2158ae69d|demo> for a more detailed walked-through and explanation of Hashy\n\n"
                "Type in `/hashy help` to get more information about using Hashy.\n\n"
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
        user_id, team_id = state.split("-")
        notion_user_id = token_response["owner"]["user"]["id"]
        access_token = token_response["access_token"]
        bot_id = token_response["bot_id"]
        workspace_id = token_response["workspace_id"]
        fields = {
            "user_id": user_id,
            "team": team_id,
            "notion_user_id": notion_user_id,
            "access_token": access_token,
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
    search_results = requests.post(
        "https://api.notion.com/v1/search",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-type": "application/json",
            "Notion-Version": "2021-08-16"
        },
        data=json.dumps(request_body)
    ).json()["results"]

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


    response = Response("success!")
    return response
