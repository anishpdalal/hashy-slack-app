from typing import Any, Dict

from sqlalchemy.orm import load_only, Session
from sqlalchemy.sql.expression import update

from . import models, schemas


def get_queries(db: Session, team: str):
    queries = db.query(models.Query).filter(models.Query.team == team).all()
    db.close()
    return queries


def get_query_by_text(db: Session, team: str, text: str):
    query = db.query(models.Query).filter(
        models.Query.team == team, models.Query.text == text
    ).first()
    return query


def create_query(db: Session, query: schemas.QueryCreate):
    query = models.Query(**query.dict())
    db.add(query)
    db.commit()
    db.refresh(query)
    db.close()
    return query


def update_query(db: Session, id: int, fields: Dict[str, Any]):
    db.query(models.Query).filter_by(id=id).update(fields)


def create_document(db: Session, doc: schemas.DocumentCreate):
    doc = models.Document(**doc.dict())
    db.add(doc)
    db.commit()
    db.refresh(doc)
    db.close()
    return doc


def get_gdrive_documents(db: Session, user: str):
    docs = db.query(models.Document).filter(
        models.Document.user == user,
        models.Document.type.contains("drive#file")).options(
        load_only(models.Document.file_id)
    ).all()
    return docs


def get_document(db: Session, file_id: str):
    doc = db.query(models.Document).filter(models.Document.file_id == file_id).first()
    return doc


def update_document(db:Session, id: int, fields: Dict[str, Any]):
    doc = db.query(models.Document).filter_by(id=id).update(fields)
    db.commit()
    db.close()
    return doc


def delete_document(db: Session, file_id: str):
    db.query(models.Document).filter(
        models.Document.file_id == file_id
    ).delete()


def get_logged_user(db: Session, user_id: str):
    user = db.query(models.LoggedUser).filter(models.LoggedUser.user_id == user_id).first()
    return user


def create_logged_user(db: Session, user: schemas.LoggedUserCreate):
    user = models.LoggedUser(**user.dict())
    return user


def create_notion_token(token: schemas.NotionTokenCreate):
    token = models.NotionToken(**token.dict())
    return token


def get_notion_token(db: Session, user_id: str):
    token = db.query(models.NotionToken).filter(models.NotionToken.user_id == user_id).first()
    return token


def update_notion_token(db:Session, id: int, fields: Dict[str, Any]):
    db.query(models.NotionToken).filter_by(id=id).update(fields)


def get_google_token(db: Session, user_id: str):
    token = db.query(models.GoogleToken).filter(models.GoogleToken.user_id == user_id).first()
    return token


def create_google_token(fields: Dict[str, Any]):
    token = models.GoogleToken(**fields)
    return token


def update_google_token(db: Session, id: int, fields: Dict[str, Any]):
    db.query(models.GoogleToken).filter_by(id=id).update(fields)