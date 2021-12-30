import base64
import io
import json
import logging
import os

import boto3
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from google.auth.transport.requests import Request as GRequest
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import requests
from slack_bolt import App
from slack_bolt.adapter.fastapi import SlackRequestHandler
from slack_bolt.oauth.oauth_settings import OAuthSettings
from slack_sdk.oauth.installation_store.sqlalchemy import SQLAlchemyInstallationStore
from slack_sdk.web import WebClient

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
    channel = body["view"]["blocks"][-1]["text"]["text"].split("Channel ID: ")[1]
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
        "user": user,
        "channel": channel
    }
    result_blocks = answer_query(event, text)
    blocks.extend(result_blocks)
    blocks.extend([
        {
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
        },
        {
            "type": "section",
            "text": {
                "type": "plain_text",
                "text": f"Channel ID: {channel}",
                "emoji": True
            }
        }
    ])
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
    channel = event["channel"]
    logger.info(json.dumps({
        "user": user,
        "team": team,
        "query": query,
        "channel": channel
    }))
    response = requests.post(
        f"{os.environ['API_URL']}/search",
        data=json.dumps({"team": team, "query": query, "user": user, "channel": channel, "count": 10})
    ).json()

    blocks = []

    if response.get("summary"):
        blocks.append({
			"type": "header",
			"text": {
				"type": "plain_text",
				"text": f"Summary",
				"emoji": True
			}
		})
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "plain_text",
                    "text": response["summary"],
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

SCOPE = 'https://www.googleapis.com/auth/drive.file'



@app.view("integration_view")
def handle_view_events(ack, body, client, view):
    integration = view["state"]["values"]["target_integration"]["target_integration_select"]["selected_option"]["value"]
    target_channel = view["state"]["values"]["target_channel"]["target_channel_select"]["selected_conversation"]
    integration, channel = integration.split("-")
    user = body["user"]["id"]
    team = body["team"]["id"]
    if integration.startswith("notion"):
        notion_id = os.environ["NOTION_CLIENT_ID"]
        redirect_uri = os.environ["NOTION_REDIRECT_URI"]
        msg = f"<https://api.notion.com/v1/oauth/authorize?owner=user&client_id={notion_id}&redirect_uri={redirect_uri}&response_type=code&state={user}-{team}-{target_channel}|Notion Integration Link>"
    else:
        google_redirect_uri = os.environ["GOOGLE_REDIRECT_URI"]
        google_client_id = os.environ["GOOGLE_CLIENT_ID"]
        msg = f"<https://accounts.google.com/o/oauth2/v2/auth?scope=https://www.googleapis.com/auth/drive.file&access_type=offline&include_granted_scopes=true&response_type=code&state={user}-{team}-{target_channel}&redirect_uri={google_redirect_uri}&client_id={google_client_id}|Google Drive Integration Link>"
    ack()
    client.chat_postMessage(channel=channel, text=msg)


@app.command("/hashy")
def help_command(ack, respond, command, client):
    ack()
    command_text = command.get("text")
    channel = command["channel_id"]
    user = command["user_id"]
    team = command["team_id"]
    notion_id = os.environ["NOTION_CLIENT_ID"]
    redirect_uri = os.environ["NOTION_REDIRECT_URI"]
    google_redirect_uri = os.environ["GOOGLE_REDIRECT_URI"]
    google_client_id = os.environ["GOOGLE_CLIENT_ID"]
    if command_text == "help":
        respond({
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"Integrate with <https://api.notion.com/v1/oauth/authorize?owner=user&client_id={notion_id}&redirect_uri={redirect_uri}&response_type=code&state={user}-{team}-{channel}|Notion>"
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"Integrate with <https://accounts.google.com/o/oauth2/v2/auth?scope=https://www.googleapis.com/auth/drive.file&access_type=offline&include_granted_scopes=true&response_type=code&state={user}-{team}-{channel}&redirect_uri={google_redirect_uri}&client_id={google_client_id}|Google Drive>."
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
    elif command_text == "integrate":
        blocks = [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "An integration can only be linked to one channel at a time. *Only share documents with your integration whose information you want viewed by members of the channel.*"
                }
		    },
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
		    },
            {
                "block_id": "target_channel",
                "dispatch_action": False,
                "type": "input",
                "element": {
                    "type": "conversations_select",
                    "placeholder": {
                        "type": "plain_text",
                        "text": "Select a channel",
                        "emoji": True
                    },
                    "action_id": "target_channel_select"
                },
                "label": {
                    "type": "plain_text",
                    "text": "Select a channel to share documents with",
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
            },
            {
                "type": "section",
                "text": {
                    "type": "plain_text",
                    "text": f"Channel ID: {channel}",
                    "emoji": True
                }
		    }
        ])
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


@api.get("/google/oauth_redirect")
async def google_authorize(req: Request, state):
    user_id, team_id, channel_id = state.split("-")
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
                "encrypted_token": credentials.refresh_token,
                "channel_id": channel_id
            }
            token = crud.create_google_token(fields)
            db.add(token)
            db.commit()
            db.refresh(token)
        else:
            fields = {
                "channel_id": channel_id
            }
            crud.update_google_token(db, token.id, fields)
            db.commit()
    except:
        db.rollback()
        raise
    finally:
        db.close()
    app_id = os.environ["GOOGLE_APP_ID"]
    key = os.environ["GOOGLE_API_KEY"]
    response = RedirectResponse(f"/google-picker/{credentials.token}?team={team_id}&user={user_id}&id={app_id}&key={key}&channel={channel_id}")
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
            var channel = urlParams.get("channel");

            function loadPicker() {
                gapi.load('picker', {'callback': onPickerApiLoad});
            }


            function onPickerApiLoad() {
                pickerApiLoaded = true;
                createPicker();
            }

            function createPicker() {
                if (pickerApiLoaded && oauthToken) {
                    var DisplayView = new google.picker.DocsView().setMimeTypes("application/vnd.google-apps.document,application/pdf,text/plain").setIncludeFolders(true);
                    var picker = new google.picker.PickerBuilder().
                        enableFeature(google.picker.Feature.MULTISELECT_ENABLED).
                        addView(DisplayView).
                        setAppId(appId).
                        setOAuthToken(oauthToken).
                        setDeveloperKey(developerKey).
                        setCallback(pickerCallback).
                        build().
                        setVisible(true);
                }
            }

            function pickerCallback(data) {
                if (data.action == google.picker.Action.PICKED) {
                    var fileIds = [];
                    for (let i = 0; i < data.docs.length; i++) {
                        fileIds.push(data.docs[i].id);
                    }
                    var xmlhttp = new XMLHttpRequest();
                    var theUrl = `https://${window.location.hostname}/google/process-documents`;
                    xmlhttp.open("POST", theUrl);
                    xmlhttp.setRequestHeader("Content-Type", "application/json");
                    xmlhttp.send(JSON.stringify({"team": team, "user": user, "channel": channel, "token": oauthToken, file_ids: fileIds}));
                    window.location.assign(`https://app.slack.com/client/${team}`);
                }
            }
            </script>
        </head>
        <body>
            <div id="result"></div>
            <!-- The Google API Loader script. -->
            <script type="text/javascript" src="https://apis.google.com/js/api.js?onload=loadPicker"></script>
        </body>
    </html>
    """
    return HTMLResponse(html)


@api.post("/google/process-documents")
def process_google_documents(upload: schemas.GooglePickerUpload):
    user_id = upload.user
    team_id = upload.team
    channel_id = upload.channel
    db = database.SessionLocal()
    token = crud.get_google_token(db, user_id)
    db.close()
    creds = Credentials.from_authorized_user_info({
        "refresh_token": token.encrypted_token,
        "client_id": os.environ["GOOGLE_CLIENT_ID"],
        "client_secret": os.environ["GOOGLE_SECRET_KEY"],
        "scopes": ["https://www.googleapis.com/auth/drive.file"]
    })
    creds.refresh(GRequest())
    service = build('drive', 'v3', credentials=creds)
    for file_id in upload.file_ids:
        file_info = service.files().get(fileId=file_id).execute()
        page = {
            "team": team_id,
            "user": user_id,
            "url": f"https://drive.google.com/file/d/{file_info['id']}",
            "filetype": f"drive#file|{file_info['mimeType']}",
            "file_name": file_info["name"],
            "file_id": file_info["id"]
        }
        queue.send_message(MessageBody=json.dumps(page))
    
    bot = installation_store.find_bot(
        enterprise_id=None,
        team_id=team_id,
    )
    client = WebClient(token=bot.bot_token)
    user = client.users_info(user=user_id)["user"]["name"]
    client.chat_postMessage(
        channel=channel_id,
        text=f"@{user} has allowed this channel to search Google Drive documents right here in Slack! Install the Hashy app and start searching from this channel with the command `/hashy <your query here>`."
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
            "workspace_id": workspace_id,
            "channel_id": channel_id
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
    user = client.users_info(user=user_id)["user"]["name"]
    client.chat_postMessage(
        channel=channel_id,
        text=f"@{user} has allowed this channel to search Notion documents right here in Slack! Install the Hashy app and start searching from this channel with the command `/hashy <your query here>`."
    )

    response = RedirectResponse(f"https://app.slack.com/client/{team_id}")
    return response
