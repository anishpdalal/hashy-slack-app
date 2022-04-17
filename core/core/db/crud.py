import os
from typing import List

from slack_sdk.oauth.installation_store.sqlalchemy import SQLAlchemyInstallationStore
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from models import ContentStore, SlackUser, Integration


user = os.environ["POSTGRES_USER"]
password = os.environ["POSTGRES_PASSWORD"]
host = os.environ["POSTGRES_HOST"]
db = os.environ["POSTGRES_DB"]
port = os.environ["POSTGRES_PORT"]

SQLALCHEMY_DATABASE_URL = f"postgresql://{user}:{password}@{host}:{port}/{db}"

engine = create_engine(SQLALCHEMY_DATABASE_URL)
Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
installation_store = SQLAlchemyInstallationStore(
    client_id=os.environ["SLACK_CLIENT_ID"],
    engine=engine
)


def get_slack_user(team_id: str, user_id: str):
    with Session() as db:
        user = db.query(SlackUser).filter(
            SlackUser.team_id == team_id,
            SlackUser.user_id == user_id
        ).first()
        return user


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


def get_most_recent_slack_content_store(channel_id: str):
    with Session() as db:
        content = db.query(ContentStore).filter(
            ContentStore.user_ids.any(channel_id),
            ContentStore.type == "slack_thread"
        ).order_by(ContentStore.source_last_updated.desc()).first()
        return content


def get_user_integrations(team_id:str, user_id: str):
    with Session() as db:
        integrations = db.query(Integration).filter(
            Integration.team_id == team_id,
            Integration.user_id == user_id,
        ).all()
        return integrations


def get_slack_integration(team_id: str):
    bot = installation_store.find_bot(
        enterprise_id=None,
        team_id=team_id,
    )
    return bot


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