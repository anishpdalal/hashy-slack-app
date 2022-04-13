import io
import json
import logging
import os

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import pdfminer
import requests
from slack_sdk.web import WebClient


logger = logging.getLogger()
logging.getLogger("pdfminer").setLevel(logging.WARNING)
logger.setLevel(logging.INFO)


def _get_notion_docs(integration):
    headers = {
        "Authorization": f"Bearer {integration.encrypted_token}",
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
    results = requests.post(
        api_url, headers=headers, data=json.dumps(request_body)
    ).json()
    docs = []
    for result in results.get("results", []):
        if result["archived"]:
            continue
        url = result["url"]
        split_url = url.split("/")[-1].split("-")
        if len(split_url) == 1:
            file_name = "Untitled"
        else:
            file_name = " ".join(split_url[:-1])
        docs.append({
            "team_id": integration.team_id,
            "user_id": integration.user_id,
            "url": url,
            "type": "notion",
            "name": file_name,
            "source_id": result["id"],
            "source_last_updated": result["last_edited_time"]
        })
    return docs


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
    service = _get_gdrive_service(integration.encrypted_token)
    start_cursor = integration.last_cursor
    if start_cursor:
        results = service.files().list(
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            pageToken=start_cursor,
            fields="nextPageToken, files(id, name, modifiedTime, mimeType)",
            orderBy="modifiedTime desc",
            q="trashed=false" 

        ).execute()
    else:
        results = service.files().list(
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            fields="nextPageToken, files(id, name, modifiedTime, mimeType)",
            orderBy="modifiedTime desc",
            q="trashed=false"
        ).execute()
    docs = []
    for result in results.get("files", []):
        source_id = result["id"]
        docs.append({
            "team_id": integration.team_id,
            "user_id": integration.user_id,
            "url": f"https://drive.google.com/file/d/{source_id}",
            "type": f"drive#file|{result['mimeType']}",
            "name": result["name"],
            "source_id": source_id,
            "source_last_updated": result["modifiedTime"]
        })
    return docs


def _get_slack_channels(integration):
    client = WebClient(token=integration.bot_token)
    channels = [channel for channel in client.conversations_list(type="public_channel")["channels"] if channel["is_member"]]
    


def list_content_stores(integration):
    if integration.type == "notion":
        return _get_notion_docs(integration)
    elif integration.type == "gdrive":
        return _get_gdrive_docs(integration)
    else:
        return []


def _get_slack_txt_document_text(token, url):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "text/html"
    }
    text = requests.get(url, headers=headers).text
    return text


def _get_slack_pdf_document_text(token, url):
    headers = {
        "Authorization": f"Bearer {token}",
    }
    byte_str = requests.get(url, headers=headers).content
    pdf_memory_file = io.BytesIO()
    pdf_memory_file.write(byte_str)
    text = pdfminer.high_level.extract_text(pdf_memory_file)
    return text


def _get_notion_document_text(file_id, token):
    try:
        api_url = f"https://api.notion.com/v1/blocks/{file_id}/children"
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


def _get_gdrive_pdf_document_text(file_id, token):
    service = _get_gdrive_service(token)
    request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
    text = pdfminer.high_level.extract_text(fh)
    return text


def _get_gdrive_document_text(file_id, token):
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


def _get_google_doc_text(file_id, token):
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
