import datetime
import json
from unittest.mock import patch

from app import get_notion_search_results
from crud import NotionToken


@patch("app.requests.post")
def test_get_notion_search_results(mock_post):
    token = NotionToken(
        id=1,
        user_id="User1",
        team="Team1",
        notion_user_id="NotionUser1",
        encrypted_token="super_secret_token",
        time_created=datetime.datetime(2020, 1, 1),
        bot_id="Bot1",
        workspace_id="workspace1",
    )
    headers = {
        "Authorization": f"Bearer {token.encrypted_token}",
        "Content-type": "application/json",
        "Notion-Version": "2021-08-16"
    }
    get_notion_search_results(token)
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
def test_get_notion_search_results_with_pagination(mock_post):
    token = NotionToken(
        id=1,
        user_id="User1",
        team="Team1",
        notion_user_id="NotionUser1",
        encrypted_token="super_secret_token",
        time_created=datetime.datetime(2020, 1, 1),
        bot_id="Bot1",
        workspace_id="workspace1",
        last_cursor="last_cursor"
    )
    headers = {
        "Authorization": f"Bearer {token.encrypted_token}",
        "Content-type": "application/json",
        "Notion-Version": "2021-08-16"
    }
    get_notion_search_results(token)
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


