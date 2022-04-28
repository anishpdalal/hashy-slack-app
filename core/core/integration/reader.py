import datetime
import io
import json
import logging
import os
import re

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import pdfminer.high_level
import requests
from slack_sdk.web import WebClient


logger = logging.getLogger()
logging.getLogger("pdfminer").setLevel(logging.WARNING)
logger.setLevel(logging.INFO)

REGEX_EXP = r"(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|\?)\s"


def _get_notion_pages(integration):
    headers = {
        "Authorization": f"Bearer {integration.token}",
        "Content-type": "application/json",
        "Notion-Version": "2021-08-16"
    }
    request_body = {
        "sort": {
            "direction": "descending",
            "timestamp": "last_edited_time"
        },
        "filter": {
            "property": "object",
            "value": "page"
        },
    }
    start_cursor = integration.last_cursor
    if start_cursor:
        request_body["start_cursor"] = start_cursor
    api_url = "https://api.notion.com/v1/search"
    notion_pages = requests.post(
        api_url, headers=headers, data=json.dumps(request_body)
    ).json()
    result = {
        "cursor": notion_pages.get("next_cursor"),
        "content_stores": []
    }
    for page in notion_pages.get("results", []):
        if page["archived"]:
            continue
        url = page["url"]
        split_url = url.split("/")[-1].split("-")
        if len(split_url) == 1:
            file_name = "Untitled"
        else:
            file_name = " ".join(split_url[:-1])
        result["content_stores"].append({
            "team_id": integration.team_id,
            "user_id": integration.user_id,
            "url": url,
            "type": "notion",
            "name": file_name,
            "source_id": page["id"],
            "source_last_updated": page["last_edited_time"]
        })
    return result


def _get_gdrive_service(token):
    creds = Credentials.from_authorized_user_info({
        "refresh_token": token,
        "client_id": os.environ["CLIENT_ID"],
        "client_secret": os.environ["CLIENT_SECRET"],
        "scopes": ["https://www.googleapis.com/auth/drive.file"]
    })
    creds.refresh(Request())
    service = build("drive", "v3", credentials=creds)
    return service


def _get_gdrive_docs(integration):
    service = _get_gdrive_service(integration.token)
    start_cursor = integration.last_cursor
    if start_cursor:
        files = service.files().list(
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            pageToken=start_cursor,
            fields="nextPageToken, files(id, name, modifiedTime, mimeType)",
            orderBy="modifiedTime desc",
            q="trashed=false" 

        ).execute()
    else:
        files = service.files().list(
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            fields="nextPageToken, files(id, name, modifiedTime, mimeType)",
            orderBy="modifiedTime desc",
            q="trashed=false"
        ).execute()
    result = {
        "cursor": files.get("next_cursor"),
        "content_stores": []
    }
    for file in files.get("files", []):
        source_id = file["id"]
        result["content_stores"].append({
            "team_id": integration.team_id,
            "user_id": integration.user_id,
            "url": f"https://drive.google.com/file/d/{source_id}",
            "type": f"drive#file|{file['mimeType']}",
            "name": file["name"],
            "source_id": source_id,
            "source_last_updated": file["modifiedTime"]
        })
    return result


def _get_slack_channels(integration):
    result = {
        "cursor": None,
        "content_stores": []
    }
    client = WebClient(token=integration.token)
    domain = client.team_info()["team"]["domain"]
    channels = []
    public_channels = client.conversations_list(type="public_channel", limit=1000)
    channels.extend(public_channels.get("channels", []))
    next_cursor = public_channels.get("response_metadata", {}).get("next_cursor")
    while next_cursor:
        public_channels = client.conversations_list(
            type="public_channel",
            limit=1000,
            cursor=next_cursor
        )
        channels.extend(public_channels.get("channels", []))
        next_cursor = public_channels.get("response_metadata", {}).get("next_cursor")
    channels = [channel for channel in channels if channel["is_member"]]
    for channel in channels:
        channel_id = channel["id"]
        latest_conversation = client.conversations_history(channel=channel_id, limit=1)
        if "messages" in latest_conversation and len(latest_conversation["messages"]) == 1:
            source_last_updated = latest_conversation["messages"][0]["ts"]
            source_last_updated = datetime.datetime.fromtimestamp(float(source_last_updated)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        else:
            source_last_updated = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        result["content_stores"].append(
            {
                "team_id": integration.team_id,
                "user_id": None,
                "url": f"https://{domain}.slack.com/archives/{channel_id}",
                "type": "slack_channel",
                "name": channel["name"],
                "source_id": channel_id,
                "source_last_updated": source_last_updated
            }
        )

    return result


def list_content_stores(integration):
    if integration.type == "notion":
        return _get_notion_pages(integration)
    elif integration.type == "gdrive":
        return _get_gdrive_docs(integration)
    elif integration.type == "slack":
        return _get_slack_channels(integration)
    else:
        return []


def _get_slack_txt_document_text(integration, content_store):
    url = content_store["url"]
    headers = {
        "Authorization": f"Bearer {integration.token}",
        "Content-Type": "text/html"
    }
    text = requests.get(url, headers=headers).text
    return text


def _get_slack_pdf_document_text(integration, content_store):
    text = None
    try:
        url = content_store["url"]
        headers = {
            "Authorization": f"Bearer {integration.token}",
        }
        byte_str = requests.get(url, headers=headers).content
        pdf_memory_file = io.BytesIO()
        pdf_memory_file.write(byte_str)
        text = pdfminer.high_level.extract_text(pdf_memory_file)
    except Exception as e:
        logger.error(e)
    return text


def _get_notion_document_text(integration, content_store):
    block_id = content_store["source_id"]
    try:
        api_url = f"https://api.notion.com/v1/blocks/{block_id}/children"
        headers = {
            "Authorization": f"Bearer {integration.token}",
            "Notion-Version": "2021-08-16"
        }
        params = {}
        child_blocks = []
        results = requests.get(api_url, headers=headers).json()
        child_blocks.extend(results["results"])
        while results.get("has_more"):
            params["start_cursor"] = results["next_cursor"]
            results = requests.get(api_url, params=params).json()
            child_blocks.extend(results["results"])
        text = []
        todos = []
        for block in child_blocks:
            try:
                if block["type"] == "paragraph":
                    for snippet in block["paragraph"]["text"]:
                        text.append(snippet["text"]["content"])
                elif block["type"] == "callout":
                    for snippet in block["callout"]["text"]:
                        text.append(snippet["text"]["content"])
                elif block["type"] == "to_do":
                    for snippet in block["to_do"]["text"]:
                        todos.append(snippet["text"]["content"])
                elif block["type"] == "bulleted_list_item":
                    for snippet in block["bulleted_list_item"]["text"]:
                        todos.append(snippet["text"]["content"])
                elif block["type"] == "numbered_list_item":
                    for snippet in block["numbered_list_item"]["text"]:
                        todos.append(snippet["text"]["content"])
                elif block["type"] == "quote":
                    for snippet in block["quote"]["text"]:
                        todos.append(snippet["text"]["content"])
                elif block["type"] == "heading_1":
                    for snippet in block["heading_1"]["text"]:
                        todos.append(snippet["text"]["content"])
                elif block["type"] == "heading_2":
                    for snippet in block["heading_2"]["text"]:
                        todos.append(snippet["text"]["content"])
                elif block["type"] == "heading_3":
                    for snippet in block["heading_3"]["text"]:
                        todos.append(snippet["text"]["content"])
                else:
                    pass
            except Exception as e:
                logger.info(e)
        todos_text = " ".join(todos)
        text.append(todos_text)
        processed_text = " ".join(" ".join(text).encode("ascii", "ignore").decode().strip().split())
    except Exception as e:
        logger.info(e)
        return ""
    return processed_text


def _get_gdrive_pdf_document_text(integration, content_store):
    text = None
    try:
        file_id = content_store["source_id"]
        service = _get_gdrive_service(integration.token)
        request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
        text = pdfminer.high_level.extract_text(fh)
    except Exception as e:
        logger.error(e)
    return text


def _get_gdrive_document_text(integration, content_store):
    file_id = content_store["source_id"]
    service = _get_gdrive_service(integration.token)
    request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
    fh.seek(0)
    text = fh.read().decode("UTF-8")
    return text


def _read_paragraph_element(element):
    """Returns the text in the given ParagraphElement.

        Args:
            element: a ParagraphElement from a Google Doc.
    """
    text_run = element.get('textRun')
    if not text_run:
        return ''
    return text_run.get('content')


def _read_strucutural_elements(elements):
    """Recurses through a list of Structural Elements to read a document's text where text may be
        in nested elements.

        Args:
            elements: a list of Structural Elements.
    """
    text = ''
    for value in elements:
        if 'paragraph' in value:
            elements = value.get('paragraph').get('elements')
            for elem in elements:
                text += _read_paragraph_element(elem)
        elif 'table' in value:
            # The text in table cells are in nested Structural Elements and tables may be
            # nested.
            table = value.get('table')
            for row in table.get('tableRows'):
                cells = row.get('tableCells')
                for cell in cells:
                    text += _read_strucutural_elements(cell.get('content'))
        elif 'tableOfContents' in value:
            # The text in the TOC is also in a Structural Element.
            toc = value.get('tableOfContents')
            text += _read_strucutural_elements(toc.get('content'))
    return text


def _get_google_doc_text(integration, content_store):
    file_id = content_store["source_id"]
    creds = Credentials.from_authorized_user_info({
        "refresh_token": integration.token,
        "client_id": os.environ["CLIENT_ID"],
        "client_secret": os.environ["CLIENT_SECRET"],
        "scopes": ["https://www.googleapis.com/auth/drive.file"]
    })
    creds.refresh(Request())
    service = build("docs", "v1", credentials=creds)
    doc = service.documents().get(documentId=file_id).execute()
    doc_content = doc.get('body').get('content')
    text = _read_strucutural_elements(doc_content)
    return text


def _should_index_slack_message(message):
    text = message.get("text")
    user = message.get("user")
    type = message.get("type")
    if not text or not user or type != "message":
        return False
    elif "?" in text and len(text.split()) >= 15:
        return True
    elif "https://" in text:
        return True
    else:
        return False
    
    

def _get_slack_channel_messages(integration, content_store):
    client = WebClient(token=integration.token)
    channel_id = content_store["source_id"]
    last_updated = content_store["source_last_updated"]
    initial_index = content_store.get("initial_index")
    if initial_index:
        messages = []
        conversations = client.conversations_history(channel=channel_id, limit=1000)
        messages.extend(conversations.get("messages", []))
        next_cursor = conversations.get("response_metadata", {}).get("next_cursor")
        while next_cursor:
            conversations = client.conversations_history(channel=channel_id, limit=1000, cursor=next_cursor)
            messages.extend(conversations.get("messages", []))
            next_cursor = conversations.get("response_metadata", {}).get("next_cursor")
    elif last_updated:
        oldest = datetime.datetime.strptime(last_updated, "%Y-%m-%dT%H:%M:%S.%fZ").timestamp()
        messages = client.conversations_history(
            channel=channel_id,
            oldest=str(oldest),
            inclusive=False
        )["messages"]
    filter_messages = []
    for message in messages:
        if not _should_index_slack_message(message):
            continue
        filter_messages.append({
            "id": f"{integration.team_id}-{message['ts']}",
            "text": message["text"],
            "user_id": message["user"],
            "team_id": content_store["team_id"],
            "text_type": "content",
            "last_updated": datetime.datetime.fromtimestamp(float(message["ts"])).strftime(
                "%Y-%m-%dT%H:%M:%S.%fZ"
            ),
            "source_name": content_store["name"],
            "source_id": content_store["source_id"],
            "source_type": "slack_message",
            "url": f"{content_store['url']}/p{message['ts'].replace('.', '')}"
            
        })
    return filter_messages


def extract_data_from_content_store(integration, content_store):
    type = content_store["type"]
    text = None
    team_id = content_store["team_id"]
    user_id = content_store["user_id"]
    if type == "slack_channel":
        return _get_slack_channel_messages(integration, content_store)
    elif type == "notion":
        text = _get_notion_document_text(integration, content_store)
    elif type == "drive#file|application/pdf":
        text = _get_gdrive_pdf_document_text(integration, content_store)
    elif type == "drive#file|application/vnd.google-apps.document":
        text = _get_google_doc_text(integration, content_store)
    elif type == "drive#file|text/plain":
        text = _get_gdrive_document_text(integration, content_store)
    elif type == "slack|application/vnd.openxmlformats-officedocument.wordprocessingml.document" or type == "slack|application/pdf":
        text = _get_slack_pdf_document_text(integration, content_store)
    elif type == "slack|text/plain":
        text = _get_slack_txt_document_text(integration, content_store)
    else:
        return []
    if not text:
        return []
    split_text = []
    chunks = re.split(REGEX_EXP, text)
    for idx, chunk in enumerate(chunks):
        split_text.append({
            "id": f"{team_id}-{content_store['source_id']}-{idx}",
            "text": chunk,
            "user_id": user_id,
            "team_id": content_store["team_id"],
            "text_type": f"content",
            "last_updated": content_store["source_last_updated"],
            "source_name": content_store["name"],
            "source_id": content_store["source_id"],
            "source_type": type,
            "url": content_store["url"]
        })
    split_text.append({
        "id": f"{team_id}-{content_store['source_id']}",
        "text": content_store["name"],
        "user_id": user_id,
        "team_id": content_store["team_id"],
        "text_type": f"title",
        "last_updated": content_store["source_last_updated"],
        "source_name": content_store["name"],
        "source_id": content_store["source_id"],
        "source_type": type,
        "url": content_store["url"]
    })
    return split_text
    
    