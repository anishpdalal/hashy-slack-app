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

view_channel_map = {}


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
    channel = view_channel_map.get(view_id)
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
    

def parse_summary(summary):
    if type(summary) == list:
        results = []
        results.append(" | ".join(list(summary[0].keys())))
        values = [" | ".join([str(val) for val in res.values()]) for res in summary]
        results.extend(values)
        return "\n".join(results)[0:3000]
    else:
        return summary
        

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
    
    if "|" in query:
        response = requests.post(
            f"{os.environ['API_URL']}/tabular-search",
            data=json.dumps({"team": team, "query": query, "user": user, "channel": channel, "count": 10})
        ).json()
    else:
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
        msg = f"<https://api.notion.com/v1/oauth/authorize?owner=user&client_id={notion_id}&redirect_uri={redirect_uri}&response_type=code&state={user}-{team}-{target_channel}|Click Here to integrate with Notion>"
    else:
        google_redirect_uri = os.environ["GOOGLE_REDIRECT_URI"]
        google_client_id = os.environ["GOOGLE_CLIENT_ID"]
        db = database.SessionLocal()
        token = crud.get_google_token(db, user)
        db.close()
        if not token or not token.encrypted_token:
            msg = f"<https://accounts.google.com/o/oauth2/v2/auth?scope=https://www.googleapis.com/auth/drive.file&access_type=offline&prompt=consent&include_granted_scopes=true&response_type=code&state={user}-{team}-{target_channel}&redirect_uri={google_redirect_uri}&client_id={google_client_id}|Click Here to integrate with Google Drive>"
        else:
            msg = f"<https://accounts.google.com/o/oauth2/v2/auth?scope=https://www.googleapis.com/auth/drive.file&access_type=offline&include_granted_scopes=true&response_type=code&state={user}-{team}-{target_channel}&redirect_uri={google_redirect_uri}&client_id={google_client_id}|Click Here to integrate with Google Drive>"
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
                        "text": f"To create or modify an integration, enter `/hashy integrate`"
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"To search documents, enter `/hashy <your query here>`"
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"To search google spreadsheets, enter `/hashy <your query here> | <name of sheet name and/or tab name>`. For example `/hashy find the customer with the most revenue in Feb 2015 | 2015 Sales`"
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"<https://www.loom.com/share/6cdd14a8ad1741939b1e990e5e111c7a?sharedAppSource=personal_library|Notion Integration Walk Through>"
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"<https://www.loom.com/share/3056708a7f4f4825a7abeaef34bd1ec1?sharedAppSource=personal_library|GDrive Integration Walk Through>"
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"<https://www.loom.com/share/82dbc6777d59420a933c720ccbe7347e?sharedAppSource=personal_library|Hashy Overview>"
                    }
                },
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
                    "text": "Select a channel to share documents with (Make sure Hashy is invited to the channel first)",
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
            }
        ])
        view_channel_map[response["view"]["id"]] = channel
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
                "<https://www.loom.com/share/82dbc6777d59420a933c720ccbe7347e?sharedAppSource=personal_library|Here's a walk through of Hashy>\n\n"
                f"1. Identify a channel you want to share your documents with and add hashy to it\n\n"
                f"2. Setup an integration with the command `/hashy integrate`\n\n"
                f"3. Search with the command `/hashy <your query here>`\n\n"
                "Type in `/hashy help` to view these commands\n\n"
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
            if credentials.refresh_token is not None:
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
                    var DisplayView = new google.picker.DocsView().
                        setMimeTypes("application/vnd.google-apps.document,application/pdf,application/vnd.google-apps.spreadsheet").
                        setIncludeFolders(true).
                        setMode(google.picker.DocsViewMode.LIST)
                    var ShareView = new google.picker.DocsView().
                        setMimeTypes("application/vnd.google-apps.document,application/pdf,application/vnd.google-apps.spreadsheet").
                        setIncludeFolders(true).
                        setMode(google.picker.DocsViewMode.LIST).
                        setEnableDrives(true)
                    var picker = new google.picker.PickerBuilder().
                        enableFeature(google.picker.Feature.MULTISELECT_ENABLED).
                        enableFeature(google.picker.Feature.SUPPORT_DRIVES).
                        addView(DisplayView).
                        addView(ShareView).
                        setAppId(appId).
                        setOAuthToken(oauthToken).
                        setDeveloperKey(developerKey).
                        setCallback(pickerCallback).
                        setTitle("Selected files will be included in the search index - Unselected files will be removed from the index").
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
                    xmlhttp.send(JSON.stringify({"team": team, "user": user, "channel": channel, "token": oauthToken, files: files}));
                    window.location.assign(`https://app.slack.com/client/${team}`);
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
    channel_id = upload.channel
    db = database.SessionLocal()
    current_docs = crud.get_gdrive_documents(db, user_id)
    db.close()
    current_file_ids = set([doc.file_id for doc in current_docs])
    uploaded_file_ids = set([file["file_id"] for file in upload.files])
    files_to_upload = [
        {
            "Id": file["file_id"],
            "MessageBody": json.dumps(
                {
                    "team": team_id,
                    "user": user_id,
                    "channel": channel_id,
                    "url": f"https://drive.google.com/file/d/{file['file_id']}",
                    "filetype": f"drive#file|{file['mime_type']}",
                    "file_name": file["file_name"],
                    "file_id": file["file_id"]
                }
            )
        } for file in upload.files if file["file_id"] not in current_file_ids
    ]
    for files_chunk in chunks(files_to_upload, batch_size=10):
        queue.send_messages(Entries=files_chunk)
    
    files_to_delete = [
        {
            "Id": file_id,
            "MessageBody": json.dumps(
                {
                    "team": team_id,
                    "user": user_id,
                    "file_id": file_id,
                    "num_vectors": crud.get_document(db, file_id).num_vectors,
                    "type": "delete"
                }
            )
        } for file_id in current_file_ids - uploaded_file_ids
    ]

    for files_chunk in chunks(files_to_delete, batch_size=10):
        queue.send_messages(Entries=files_chunk)


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
    
    lambda_client = boto3.client("lambda", region_name="us-east-1")
    payload = {"team": team_id, "user": user_id}
    lambda_client.invoke(
        FunctionName=os.environ["LAMBDA_FUNCTION"],
        InvocationType="Event",
        Payload=json.dumps(payload)
    )

    response = RedirectResponse(f"https://app.slack.com/client/{team_id}")
    return response
