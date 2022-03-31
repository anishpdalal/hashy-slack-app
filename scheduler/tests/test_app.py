import datetime
import json
from unittest.mock import patch

import pytest

from app import (
    get_notion_search_results,
    get_google_search_results,
    get_tokens
)
from crud import GoogleToken, NotionToken


@pytest.fixture
def notion_token():
    token = NotionToken(
        id=1,
        user_id="User1",
        team="Team1",
        notion_user_id="NotionUser1",
        encrypted_token="super_secret_token",
        time_created=datetime.datetime(2020, 1, 1),
        bot_id="Bot1",
        workspace_id="workspace1",
        last_cursor=None
    )
    return token


@pytest.fixture
def google_token():
    token = GoogleToken(
        id=1,
        user_id="User1",
        team="Team1",
        encrypted_token="super_secret_token",
        time_created=datetime.datetime(2020, 1, 1),
        last_cursor=None
    )
    return token


@patch("app.requests.post")
def test_get_notion_search_results(mock_post, notion_token):
    headers = {
        "Authorization": f"Bearer {notion_token.encrypted_token}",
        "Content-type": "application/json",
        "Notion-Version": "2021-08-16"
    }
    get_notion_search_results(notion_token)
    mock_post.assert_called_once_with(
        "https://api.notion.com/v1/search",
        headers=headers,
        data=json.dumps(
            {
                "sort": {
                    "direction": "descending",
                    "timestamp": "last_edited_time"
                },
                "filter": {
                    "property": "object",
                    "value": "page"
                }
            }
        )
    )


@patch("app.requests.post")
def test_get_notion_search_results_with_pagination(mock_post, notion_token):
    notion_token.last_cursor = "last_cursor"
    headers = {
        "Authorization": f"Bearer {notion_token.encrypted_token}",
        "Content-type": "application/json",
        "Notion-Version": "2021-08-16"
    }
    get_notion_search_results(notion_token)
    mock_post.assert_called_once_with(
        "https://api.notion.com/v1/search",
        headers=headers,
        data=json.dumps(
            {
                "sort": {
                    "direction": "descending",
                    "timestamp": "last_edited_time"
                },
                "filter": {
                    "property": "object",
                    "value": "page"
                },
                "start_cursor": "last_cursor"
            }
        )
    )


@patch("app.get_drive_service")
def test_get_google_search_results(mock_get_service, google_token):
    mock_get_service.return_value.files.return_value.list.return_value\
        .execute.return_value = None
    get_google_search_results(google_token)
    mock_get_service.return_value.files.return_value.list\
        .assert_called_once_with(
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            fields="nextPageToken, files(id, name, modifiedTime, mimeType)",
            orderBy="modifiedTime desc"
        )


@patch("app.get_drive_service")
def test_get_google_search_results_with_pagination(
    mock_get_service,
    google_token
):
    google_token.last_cursor = "last_cursor"
    mock_get_service.return_value.files.return_value.list.return_value\
        .execute.return_value = None
    get_google_search_results(google_token)
    mock_get_service.return_value.files.return_value.list\
        .assert_called_once_with(
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            pageToken="last_cursor",
            fields="nextPageToken, files(id, name, modifiedTime, mimeType)",
            orderBy="modifiedTime desc"
        )


@patch("app.crud.get_google_token")
@patch("app.crud.get_notion_token")
@patch("app.sessionmaker")
def test_get_user_tokens(
    sessionmaker,
    get_notion_token,
    get_google_token,
    google_token,
    notion_token
):
    mock_db = sessionmaker()()
    get_notion_token.return_value = notion_token
    get_google_token.return_value = google_token
    user = "User1"
    team = "Team1"
    tokens = get_tokens(mock_db, user, team)
    assert len(tokens) == 2
    get_notion_token.assert_called_once_with(mock_db, user, team)
    get_google_token.assert_called_once_with(mock_db, user, team)


@patch("app.crud.get_google_tokens")
@patch("app.crud.get_notion_tokens")
@patch("app.sessionmaker")
def test_get_all_tokens(
    sessionmaker,
    get_notion_tokens,
    get_google_tokens,
    google_token,
    notion_token
):
    mock_db = sessionmaker()()
    get_notion_tokens.return_value = [notion_token]
    get_google_tokens.return_value = [google_token]
    user = None
    team = None
    tokens = get_tokens(mock_db, user, team)
    assert len(tokens) == 2
    get_notion_tokens.assert_called_once_with(mock_db)
    get_google_tokens.assert_called_once_with(mock_db)
