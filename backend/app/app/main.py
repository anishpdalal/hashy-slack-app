import base64
import datetime
import itertools
import json
import logging
import os
import re
from typing import Any, List
from urllib.parse import urlparse
import uuid

import boto3
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from google_auth_oauthlib.flow import Flow
from pydantic import BaseModel
import requests
from slack_bolt import App
from slack_bolt.adapter.fastapi import SlackRequestHandler
from slack_bolt.oauth.oauth_settings import OAuthSettings
from slack_sdk.oauth.installation_store.sqlalchemy import SQLAlchemyInstallationStore
from slack_sdk.web import WebClient
from slack_sdk.errors import SlackApiError

from core.db import crud

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


installation_store = SQLAlchemyInstallationStore(
    client_id=os.environ["SLACK_CLIENT_ID"],
    engine=crud.engine
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


@app.event({"type": "message", "subtype": "message_deleted"})
def handle_message_deleted(event, say):
    pass


@app.event("member_joined_channel")
def handle_member_join(client, event, say):
    user_id = event["user"]
    team_id = event["team"]
    channel_id = event["channel"]
    bot = installation_store.find_bot(
        enterprise_id=None,
        team_id=team_id,
    )
    bot_user_id = bot.bot_user_id
    channel_content_store = crud.get_content_store(channel_id)
    if bot_user_id == user_id and not channel_content_store:
        domain = client.team_info()["team"]["domain"]
        channel_name = client.conversations_info(channel=channel_id)["channel"]["name"]
        latest_conversation = client.conversations_history(channel=channel_id, limit=1)
        if "messages" in latest_conversation and len(latest_conversation["messages"]) == 1:
            source_last_updated = latest_conversation["messages"][0]["ts"]
            source_last_updated = datetime.datetime.fromtimestamp(float(source_last_updated)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        else:
            source_last_updated = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        integration = crud.get_user_integration(team_id, None, "slack")
        message = {
            "integration_id": integration.id,
            "team_id": team_id,
            "user_id": None,
            "url": f"https://{domain}.slack.com/archives/{channel_id}",
            "type": "slack_channel",
            "name": channel_name,
            "source_id": channel_id,
            "source_last_updated": source_last_updated,
            "initial_index": True
        }
        queue.send_message(MessageBody=json.dumps(message))


ACCEPTED_FILE_FORMATS = [
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "application/pdf",
    "text/plain",
]


@app.event({"type": "message", "subtype": "file_share"})
def handle_message_file_share(logger, event, say):
    pass


@app.event({"type": "message"})
def handle_message_channel(event, say, client):
    if event.get("channel_type") == "channel" and not event.get("parent_user_id"):
        channel = event["channel"]
        ts = event["ts"]
        team = event.get("team")
        user = event.get("user")
        query = event.get("text")
        cleaned_query = re.sub(r'http\S+', '', query) if query else None
        if (query and "?" in cleaned_query and user and team) or (query and user and team == "T02KCNMCUHE") or (query and user and team == "T015E1A6N6L") or (query and user and team == "T02MGVB1HL5"):
            query_id = str(uuid.uuid4())
            response = requests.post(
                f"{os.environ['API_URL']}/search",
                data=json.dumps(
                    {
                        "query_id": query_id,
                        "team_id": team,
                        "query": query,
                        "user_id": user,
                        "count": 10,
                        "event_type": "CHANNEL_SEARCH"
                    }
                )
            ).json()
            modified_query = response.get("modified_query")
            slack_message_results = response.get("slack_messages_results", [])
            content_results = response.get("content_results", [])
            source_ids = [result["source_id"] for result in content_results]
            content_stores = crud.get_content_stores(source_ids) or []
            content_store_user_mapping = {content_store.source_id: content_store.is_boosted for content_store in content_stores}
            filtered_content_results = [result for result in content_results if content_store_user_mapping.get(result["source_id"])]
            results = filtered_content_results + slack_message_results
            if len(results) > 0:
                blocks = []
                top_result = results[0]
                if top_result["source_type"] == "slack_message":
                    channel_link = f"<{top_result['url']}|{top_result['name']}>"
                    blocks.append(
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": f"Found a related slack conversation in {channel_link} for {modified_query}"
                            }
                        }
                    )
                elif top_result["source_type"] == "answer":
                    source_name = top_result["name"]
                    question = top_result["text"]
                    answer = top_result["answer"]
                    blocks.append(
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": f"Found a related answer from {source_name} \n\n_Answer_: {answer}\n\n_Topic_: {question}"
                            }
                        }
                    )
                else:
                    source_name = top_result["name"]
                    source_url = top_result["url"]
                    source_text = top_result["text"]
                    blocks.append(
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": f"Found related content from <{source_url}|{source_name}> \n\n{source_text}"
                            }
                        }
                    )
                blocks.extend([
                    {
                        "type": "actions",
                        "elements": [
                            {
                                "type": "button",
                                "text": {
                                    "type": "plain_text",
                                    "text": "View More",
                                    "emoji": True
                                },
                                "value": modified_query,
                                "action_id": "view_more_button"
                            }
			            ]
		            },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": "Rate the response to improve Hashy"
                        }
		            },
                    {
                        "type": "actions",
                        "elements": [
                            {
                                "type": "button",
                                "text": {
                                    "type": "plain_text",
                                    "text": ":thumbsup:",
                                    "emoji": True
                                },
                                "value": query_id,
                                "action_id": "upvote"
                            },
                            {
                                "type": "button",
                                "text": {
                                    "type": "plain_text",
                                    "text": ":thumbsdown:",
                                    "emoji": True
                                },
                                "value": query_id,
                                "action_id": "downvote"
                            }
                        ]
                    }
                ])
                client.chat_postMessage(channel=channel, thread_ts=ts, blocks=blocks)


@app.action("upvote")
def handle_engagement_click(ack, body, client):
    ack()
    query_id = body["actions"][0]["value"]
    user_id = body["user"]["id"]
    team_id = body["team"]["id"]
    event_type = "UPVOTE"
    requests.post(
        f"{os.environ['API_URL']}/ping",
        data=json.dumps(
            {
                "query_id": query_id,
                "team_id": team_id,
                "user_id": user_id,
                "event_type": event_type
            }
        )
    )

@app.action("downvote")
def handle_engagement_click(ack, body, client):
    ack()
    query_id = body["actions"][0]["value"]
    user_id = body["user"]["id"]
    team_id = body["team"]["id"]
    event_type = "DOWNVOTE"
    requests.post(
        f"{os.environ['API_URL']}/ping",
        data=json.dumps(
            {
                "query_id": query_id,
                "team_id": team_id,
                "user_id": user_id,
                "event_type": event_type
            }
        )
    )


@app.action("view_more_button")
def handle_view_more_click(ack, body, client):
    ack()
    trigger_id = body["trigger_id"]
    user = body["user"]["id"]
    team = body["team"]["id"]
    query = body["actions"][0]["value"]
    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"Query: {query}"
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
        trigger_id=trigger_id,
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
    event = {
        "user": user,
        "team": team
    }
    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"Query: {query}"
            }
        }
    ]
    result_blocks = answer_query(event, query, type="AUTO_REPLY_CLICK")
    blocks.extend(result_blocks)
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
            "close": {
                "type": "plain_text",
                "text": "Close",
                "emoji": True
            },
            "clear_on_close": True
        }
    )


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
        message = {
            "team_id": team,
            "user_id": user,
            "type": "answer",
            "source_id": str(uuid.uuid4()),
            "source_name": user_name,
            "source_last_updated": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "text": text,
            "answer": answer
        }
        queue.send_message(MessageBody=json.dumps(message))
        client.chat_postMessage(
            channel=user,
            text=f"Successfully saved answer to {text}"
        )
    for id in selected_conversations:
        slack_user = crud.get_slack_user(team, user)
        if slack_user:
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


def answer_query(event, query, type=None):
    team = event["team"]
    user = event["user"]
    response = requests.post(
        f"{os.environ['API_URL']}/search",
        data=json.dumps(
            {
                "query_id": str(uuid.uuid4()),
                "team_id": team,
                "query": query,
                "user_id": user,
                "count": 10,
                "event_type": type
            }
        )
    ).json()
    slack_messages_results = response.get("slack_messages_results", [])
    content_results = response.get("content_results", [])
    title_results = response.get("title_results", [])
    summarized_result = response.get("summarized_result")
    all_results = slack_messages_results + content_results
    source_ids = [result["source_id"] for result in content_results + title_results]
    content_stores = crud.get_content_stores(source_ids) or []
    content_store_user_mapping = {content_store.source_id: {"users": content_store.user_ids, "boosted": content_store.is_boosted} for content_store in content_stores}
    if all_results:
        top_result = max(slack_messages_results + content_results, key=lambda x: x["semantic_score"])
        top_score = int(top_result["semantic_score"] * 100)  
        days_old = (datetime.datetime.now() - datetime.datetime.strptime(top_result["last_updated"], "%m/%d/%Y")).days
        freshness_score = top_score + 10 if days_old <= 100 else top_score
        freshness_score = min(freshness_score, 95)
    else:
        freshness_score = 0
    gdrive_integration = crud.get_user_integration(team, user, "gdrive")
    notion_integration = crud.get_user_integration(team, user, "notion")
    blocks = []
    blocks.append({
        "type": "header",
        "text": {
            "type": "plain_text",
            "text": f"Knowledge Freshness Score: {freshness_score}",
            "emoji": True
        }
    })
    if summarized_result and "I don't know" not in summarized_result:
        blocks.append({
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"Summarized Answer",
                "emoji": True
            }
        })
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "plain_text",
                    "text": summarized_result,
                    "emoji": True
                }
            }
        )
        blocks.append({"type": "divider"})

    if slack_messages_results:
        blocks.append({
			"type": "header",
			"text": {
				"type": "plain_text",
				"text": f"Team Answers",
				"emoji": True
			}
		})
    
    for idx, result in enumerate(slack_messages_results):
        if idx != 0:
            blocks.append({"type": "divider"})
        if result["source_type"] == "slack_message":
            question = result["text"]
            source = f"<{result['url']}|{result['name']}>"
            last_updated = result["last_updated"]
            blocks.append(
                {
                    "block_id": result["id"],
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"\n\n_Responding to_: {question}\n\n _Source_: {source} on {last_updated}\n\n"
                    }
                }
            )
        elif result["source_type"] == "answer":
            question = result["text"]
            answer = result["answer"]
            last_updated = result["last_updated"]
            source = result["name"]
            blocks.append(
                {
                    "block_id": result["id"],
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"\n\n_Responding to_: {question}\n\n _Answer_: {answer}\n\n _Source_: {source} on {last_updated}"
                    }
                }
            )
        else:
            pass

    boosted_content_results = [res for res in content_results if content_store_user_mapping.get(result["source_id"], {}).get("boosted", False)]
    non_boosted_content_results = [res for res in content_results if not content_store_user_mapping.get(result["source_id"], {}).get("boosted", False)]
    content_results = boosted_content_results + non_boosted_content_results
    if content_results:
        blocks.append({
			"type": "header",
			"text": {
				"type": "plain_text",
				"text": f"Document Snippets",
				"emoji": True
			}
		})
        if not gdrive_integration and not notion_integration:
            blocks.append(
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"Set up an integration to view results from documents with the `/hashy integrate` command"
                    }
                }
            )
    for idx, result in enumerate(content_results):
        content_source_id = result["source_id"]
        if user not in content_store_user_mapping.get(content_source_id, {}).get("users", []) \
                and not content_store_user_mapping.get(content_source_id, {}).get("boosted", False):
            continue
        if idx != 0:
            blocks.append({"type": "divider"})
        name = result["name"]
        url = result["url"]
        text = result["text"]
        last_updated = result["last_updated"]
        source_type = result["source_type"]
        if source_type.startswith("notion"):
            source_type = "(Notion)"
        elif source_type.startswith("drive"):
            source_type = "(Google Drive)"
        else:
            source_type = ""
        source = "Notion" if source_type.startswith("notion") else "Google Drive"
        source_text = f"<{url}|{name}> {source_type} on {last_updated}"
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "plain_text",
                    "text": text,
                    "emoji": True
                }
            }
        )
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"Source: {source_text}"
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
    if integration.startswith("notion"):
        notion_id = os.environ["NOTION_CLIENT_ID"]
        redirect_uri = os.environ["NOTION_REDIRECT_URI"]
        msg = f"<https://api.notion.com/v1/oauth/authorize?owner=user&client_id={notion_id}&redirect_uri={redirect_uri}&response_type=code&state={user}-{team}|Click Here to integrate with Notion>"
    else:
        google_redirect_uri = os.environ["GOOGLE_REDIRECT_URI"]
        google_client_id = os.environ["GOOGLE_CLIENT_ID"]
        gdrive_integration = crud.get_user_integration(team, user, "gdrive")
        if not gdrive_integration or not gdrive_integration.token:
            msg = f"<https://accounts.google.com/o/oauth2/v2/auth?scope=https://www.googleapis.com/auth/drive.file&access_type=offline&prompt=consent&include_granted_scopes=true&response_type=code&state={user}-{team}&redirect_uri={google_redirect_uri}&client_id={google_client_id}|Click Here to integrate with Google Drive>"
        else:
            msg = f"<https://accounts.google.com/o/oauth2/v2/auth?scope=https://www.googleapis.com/auth/drive.file&access_type=offline&include_granted_scopes=true&response_type=code&state={user}-{team}&redirect_uri={google_redirect_uri}&client_id={google_client_id}|Click Here to integrate with Google Drive>"
    ack()
    client.chat_postMessage(channel=channel, text=msg)


@app.view("contribute_answer_view")
def contribute_answer_view(ack, body, client, view):
    question = view["state"]["values"]["save_question"]["save_question"]["value"]
    answer = view["state"]["values"]["save_answer"]["save_answer"]["value"]
    user = body["user"]["id"]
    team = body["team"]["id"]
    user_name = client.users_info(user=user)["user"]["name"]
    ack()
    message = {
        "team_id": team,
        "user_id": user,
        "type": "answer",
        "source_id": str(uuid.uuid4()),
        "source_name": user_name,
        "source_last_updated": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "text": question,
        "answer": answer
    }
    queue.send_message(MessageBody=json.dumps(message))
    client.chat_postMessage(channel=user, text=f"Successfully saved answer to {question}")


@app.shortcut("contribute_answer")
def contribute_answer_shortcuts(ack, body, client):
    ack()
    trigger_id = body["trigger_id"]
    text = body["message"]["text"]
    blocks = [
        {
            "block_id": "save_question",
            "type": "input",
            "optional": False,
            "element": {
                "type": "plain_text_input",
                "action_id": "save_question",
            },
            "label": {
                "type": "plain_text",
                "text": "Enter question or topic",
                "emoji": True
            }
        },
        {
            "block_id": "save_answer",
            "type": "input",
            "optional": False,
            "element": {
                "type": "plain_text_input",
                "action_id": "save_answer",
                "multiline": True,
                "initial_value": text
            },
            "label": {
                "type": "plain_text",
                "text": "Enter answer to share with your team",
                "emoji": True
            }
        }
    ]
    client.views_open(
        trigger_id=trigger_id,
        view={
            "callback_id": "contribute_answer_view",
            "type": "modal",
            "title": {
                "type": "plain_text",
                "text": f"Contribute Answer",
                "emoji": True
            },
            "blocks": blocks,
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
        },
    )


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
                        "text": f"To make a document into a public key document, enter `/hashy share <document url>`"
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
    elif command_text.startswith("share"):
        content_store_url = command_text.split(" ")[1]
        parsed_url = urlparse(content_store_url)

        if "notion.so" in parsed_url.netloc:
            source_id = str(uuid.UUID(parsed_url.path.split("/")[-1]))
        elif "google.com" in parsed_url.netloc:
            path = parsed_url.path
            split_path = path.split("/")
            idx = split_path.index("d") + 1
            source_id = split_path[idx]
        else:
            client.chat_postMessage(channel=user, text=f"Could not process document: {content_store_url}.")
            return
        content_store = crud.get_content_store(source_id)
        users = content_store.user_ids
        if user not in users:
            client.chat_postMessage(channel=user, text=f"Could not add document as key doc. It needs to be shared with your integration.")
            return
        crud.update_content_store(source_id, {"is_boosted": True})
        client.chat_postMessage(channel=user, text=f"Added key doc")

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
        result_blocks = answer_query(event, command_text, type="COMMAND_SEARCH")
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
    result = client.users_info(user=user_id)
    team_id = result["user"]["team_id"]
    slack_token = crud.get_user_integration(team_id, None, "slack")
    if slack_token is None:
        bot = installation_store.find_bot(
            enterprise_id=None,
            team_id=team_id,
        )
        crud.create_integration({
            "team_id": team_id,
            "type": "slack",
            "token": bot.bot_token
        })
    slack_user = crud.get_slack_user(team_id, user_id)
    if slack_user is None:
        team_info = client.team_info(team=team_id)
        team_name = team_info["team"]["name"]
        crud.create_slack_user({
            "user_id": user_id,
            "team_name": team_name,
            "team_id": team_id
        })
        say(f":wave: Hi <@{result['user']['name']}>, Welcome to the Hashy app on Slack! Hashy lets you quickly find and share knowledge across your org.\n")

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
    integration = crud.get_user_integration(team_id, user_id, "gdrive")
    if not integration:
        fields = {
            "team_id": team_id,
            "type": "gdrive",
            "token": credentials.refresh_token,
            "user_id": user_id
        }
        crud.create_integration(fields)
    else:
        if credentials.refresh_token is not None:
            fields["token"] = credentials.refresh_token
            crud.update_integration(integration.id, fields)

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
                            "mime_type": data.docs[i].mimeType,
                            "source_last_updated": data.docs[i].lastEditedUtc
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


class GooglePickerUpload(BaseModel):
    files: List[Any]
    team: str
    user: str
    token: str


@api.post("/google/process-documents")
def process_google_documents(upload: GooglePickerUpload):
    user_id = upload.user
    team_id = upload.team
    integration = crud.get_user_integration(team_id, user_id, "gdrive")
    files_to_upload = [
        {
            "Id": file["file_id"],
            "MessageBody": json.dumps(
                {
                    "integration_id": integration.id,
                    "team_id": team_id,
                    "user_id": user_id,
                    "url": f"https://drive.google.com/file/d/{file['file_id']}",
                    "type": f"drive#file|{file['mime_type']}",
                    "name": file["file_name"],
                    "source_id": file["file_id"],
                    "source_last_updated": datetime.datetime.fromtimestamp(
                        file["source_last_updated"] / 1000
                    ).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
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
    user_id, team_id = state.split("-")
    notion_user_id = token_response["owner"]["user"]["id"]
    bot_id = token_response["bot_id"]
    workspace_id = token_response["workspace_id"]
    access_token = token_response["access_token"]
    extra = json.dumps({
        "notion_user_id": notion_user_id,
        "bot_id": bot_id,
        "workspace_id": workspace_id
    })
    fields = {
        "team_id": team_id,
        "type": "notion",
        "token": access_token,
        "user_id": user_id,
        "extra": extra
    }
    integration = crud.get_user_integration(team_id, user_id, "notion")
    if integration:
        crud.update_integration(integration.id, fields)
    else:
        crud.create_integration(fields)

    bot = installation_store.find_bot(
        enterprise_id=None,
        team_id=team_id,
    )
    client = WebClient(token=bot.bot_token)
    client.chat_postMessage(
        channel=user_id,
        text="Integrating your notion documents! It may take up to a few hours to process all of them. Once "
        "they are ready we'll send you a message to notify you."
    )
    response = RedirectResponse(f"https://app.slack.com/client/{team_id}")
    return response