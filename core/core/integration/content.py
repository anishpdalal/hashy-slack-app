from datetime import datetime
import io
import json
import logging
import os

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import pdfminer
import pytz
import requests
from slack_sdk.web import WebClient

from db import crud


logger = logging.getLogger()
logging.getLogger("pdfminer").setLevel(logging.WARNING)
logger.setLevel(logging.INFO)


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
            "type": f"drive#file|{result['mimeType']}",
            "name": file["name"],
            "source_id": source_id,
            "source_last_updated": file["modifiedTime"]
        })
    return result


def _get_slack_threads(integration):
    client = WebClient(token=integration.token)
    public_channels = client.conversations_list(type="public_channel")["channels"]
    public_channels = [channel for channel in public_channels if channel["is_member"]]
    result = {
        "cursor": None,
        "content_stores": []
    }
    last_cursor = integration.last_cursor
    current_channel_cursors = json.loads(last_cursor) if last_cursor else {}
    new_channel_cursors = {}
    messages = []
    for channel in public_channels:
        channel_id = channel["id"]
        channel_name = channel["name"]
        domain = client.team_info(channel=channel_id)["team"]["domain"]
        most_recent_thread = crud.get_most_recent_slack_content_store(channel_id)
        if most_recent_thread:
            thread_id = most_recent_thread.source_id
            recent_conversations = client.conversations_history(
                channel=channel_id,
                include_all_metadata=True,
                inclusive=False,
                oldest=thread_id
            )
            messages.extend(recent_conversations.get("messages", []))
        historical_conversations = client.conversations_history(
            channel=channel_id,
            include_all_metadata=True,
            cursor = current_channel_cursors.get(channel_id)
        )
        new_channel_cursors[channel_id] = historical_conversations["response_metadata"].get("next_cursor")
        messages.extend(historical_conversations.get("messages", []))
    threads = []
    for message in messages:
        element_type = None
        num_reply_users = len([user for user in message.get("reply_users", []) if user.startswith("U")])
        blocks = message.get("blocks", [])
        if len(blocks) > 0:
            first_block = blocks[0]
            first_element = first_block["elements"][0] if "elements" in first_block and type(first_block["elements"]) == list else None
            element_type = first_element["elements"][0]["type"] if "elements" in first_element and type(first_element["elements"]) == list else None
        if num_reply_users > 0 and (element_type == "text" or element_type == "broadcast"):
            source_id = message["ts"]
            threads.append({
                "team_id": integration.team_id,
                "user_id": channel_id,
                "url": f"<https://{domain}.slack.com/archives/{channel_id}/{source_id}|{channel_name}>",
                "type": "slack_thread",
                "name": f"{channel_name}_{message['ts']}",
                "source_id": source_id,
                "source_last_updated": pytz.utc.localize(datetime.fromtimestamp(float(message["ts"]))).strftime(
                    "%Y-%m-%dT%H:%M:%S.%fZ"
                )
            })
    result["content_stores"] = threads
    result["cursor"] = json.dumps(new_channel_cursors)
    return result


def list_content_stores(integration):
    if integration.type == "notion":
        return _get_notion_pages(integration)
    elif integration.type == "gdrive":
        return _get_gdrive_docs(integration)
    elif integration.type == "slack":
        return _get_slack_threads(integration)
    else:
        return []


def _get_slack_txt_document_text(token, content_store):
    url = content_store["url"]
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "text/html"
    }
    text = requests.get(url, headers=headers).text
    return text


def _get_slack_pdf_document_text(token, content_store):
    url = content_store["url"]
    headers = {
        "Authorization": f"Bearer {token}",
    }
    byte_str = requests.get(url, headers=headers).content
    pdf_memory_file = io.BytesIO()
    pdf_memory_file.write(byte_str)
    text = pdfminer.high_level.extract_text(pdf_memory_file)
    return text


def _get_notion_document_text(token, content_store):
    block_id = content_store["source_id"]
    try:
        api_url = f"https://api.notion.com/v1/blocks/{block_id}/children"
        headers = {
            "Authorization": f"Bearer {token}",
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
        todos_text = ". ".join(todos)
        text.append(todos_text)
        processed_text = " ".join(" ".join(text).encode("ascii", "ignore").decode().strip().split())
    except Exception as e:
        logger.info(e)
        return ""
    return processed_text


def _get_gdrive_pdf_document_text(token, content_store):
    file_id = content_store["source_id"]
    service = _get_gdrive_service(token)
    request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
    text = pdfminer.high_level.extract_text(fh)
    return text


def _get_gdrive_document_text(token, content_store):
    file_id = content_store["source_id"]
    service = _get_gdrive_service(token)
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


def _get_google_doc_text(token, content_store):
    file_id = content_store["source_id"]
    creds = Credentials.from_authorized_user_info({
        "refresh_token": token,
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


def get_content_from_store(integration, content_store):
    token = integration.token
    type = content_store.type
    text = None
    if type == "notion":
        text = _get_notion_document_text(token, content_store)
    elif type == "drive#file|application/pdf":
        text = _get_gdrive_pdf_document_text(token, content_store)
    elif type == "drive#file|application/vnd.google-apps.document":
        text = _get_google_doc_text(token, content_store)
    elif type == "drive#file|text/plain":
        text = _get_gdrive_document_text(token, content_store)
    elif type == "docx" or type == "application/pdf":
        text = _get_slack_pdf_document_text(token, content_store)
    