# import os

from fastapi import FastAPI, Request
from slack_bolt import App
from slack_bolt.adapter.fastapi import SlackRequestHandler
# import requests
# import spacy

app = App()
app_handler = SlackRequestHandler(app)

# nlp = spacy.load("en_core_web_sm")


# @app.event("file_created")
# def handle_file_created_events(event, say):
#     pass


# def _get_file_text(url):
#     headers = {
#         "Authorization": f"Bearer {os.environ['SLACK_BOT_TOKEN']}"
#     }
#     text = requests.get(url, headers=headers).text
#     return text


# def _process_file(file):
#     text = _get_file_text(file["url_private"])
#     doc = nlp(text)

# @app.event("message")
# def handle_message_events(event, say):
#     print(event)
#     if event["subtype"] == "file_share":
#         file = event["files"][0]
#         if file["mimetype"] == "text/plain":
#             text = _get_file_text(file["url_private"])
#             # doc = nlp(r.text)
#             # print(list(doc.sents))
#             # for sent in doc.sents:
#             #     print(sent)
#             # say(f"File {file['name']} processed!")


api = FastAPI()


@api.post("/slack/events")
async def endpoint(req: Request):
    return await app_handler.handle(req)
