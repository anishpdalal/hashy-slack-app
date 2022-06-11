from datetime import datetime, timedelta
import os
from typing import List

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from .models import ContentStore, SlackUser, Integration


user = os.environ["POSTGRES_USER"]
password = os.environ["POSTGRES_PASSWORD"]
host = os.environ["POSTGRES_HOST"]
db = os.environ["POSTGRES_DB"]
port = os.environ["POSTGRES_PORT"]

SQLALCHEMY_DATABASE_URL = f"postgresql://{user}:{password}@{host}:{port}/{db}"

engine = create_engine(SQLALCHEMY_DATABASE_URL)
Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_slack_user(team_id: str, user_id: str):
    with Session() as db:
        user = db.query(SlackUser).filter(
            SlackUser.team_id == team_id,
            SlackUser.user_id == user_id
        ).first()
        return user


def create_slack_user(slack_user: dict):
    with Session() as db:
        try:
            slack_user = SlackUser(**slack_user)
            db.add(slack_user)
        except:
            db.rollback()
            raise
        else:
            db.commit()


def update_slack_user(id: int, fields: dict):
    with Session() as db:
        try:
            db.query(SlackUser).filter(
                SlackUser.id == id
            ).update(fields)
        except:
            db.rollback()
            raise
        else:
            db.commit()


def get_content_stores(source_ids: List[str]):
    with Session() as db:
        content = db.query(ContentStore).filter(
            ContentStore.source_id.in_(source_ids)
        ).all()
        return content


def get_content_store(source_id: str):
    with Session() as db:
        content = db.query(ContentStore).filter(
            ContentStore.source_id == source_id
        ).first()
        return content


def create_content_store(content: dict):
    with Session() as db:
        try:
            content = ContentStore(**content)
            db.add(content)
        except:
            db.rollback()
            raise
        else:
            db.commit()


def update_content_store(source_id: str, fields: dict):
    with Session() as db:
        try:
            db.query(ContentStore).filter(
                ContentStore.source_id == source_id
            ).update(fields)
        except:
            db.rollback()
            raise
        else:
            db.commit()


def get_older_content_stores_from_integration(integration, days=180):
    if integration.type == "slack":
        content_store_type = "slack_message"
    elif integration.type == "gdrive":
        content_store_type = "drive#file|application/vnd.google-apps.document"
    elif integration.type == "notion":
        content_store_type = "notion"
    else:
        return
    with Session() as db:
        content = db.query(ContentStore).filter(
            ContentStore.team_id == integration.team_id,
            ContentStore.type == content_store_type,
            ContentStore.source_last_updated < datetime.today() - timedelta(days=days),
            ContentStore.is_boosted == False
        ).all()
        return content


def delete_content_stores(source_ids: List[str]):
    with Session() as db:
        try:
            db.query(ContentStore).filter(
                ContentStore.source_id.in_(source_ids)
            ).delete()
        except:
            db.rollback()
            raise
        else:
            db.commit()


def get_all_integrations():
    with Session() as db:
        integrations = db.query(Integration).order_by(
                Integration.updated.asc()
            ).all()
        return integrations


def get_integration(id: int):
    with Session() as db:
        integration = db.query(Integration).filter(Integration.id == id).first()
        return integration


def get_user_integration(team_id: str, user_id: str, type: str):
    with Session() as db:
        integration = db.query(Integration).filter(
            Integration.team_id == team_id,
            Integration.user_id == user_id,
            Integration.type == type,
        ).first()
        return integration


def create_integration(integration: dict):
    with Session() as db:
        try:
            integration = Integration(**integration)
            db.add(integration)
        except:
            db.rollback()
            raise
        else:
            db.commit()


def update_integration(id: int, fields: dict):
    with Session() as db:
        try:
            db.query(Integration).filter(
                Integration.id == id
            ).update(fields)
        except:
            db.rollback()
            raise
        else:
            db.commit()


def dispose_engine():
    engine.dispose()