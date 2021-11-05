from typing import Any, Dict

from sqlalchemy.orm import Session
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
    db.close()
    return query


def create_query(db: Session, query: schemas.QueryCreate):
    query = models.Query(**query.dict())
    db.add(query)
    db.commit()
    db.refresh(query)
    db.close()
    return query


def update_query(db: Session, id: int, fields: Dict[str, Any]):
    query = db.query(models.Query).filter_by(id=id).update(fields)
    db.commit()
    db.close()
    return query


def create_document(db: Session, doc: schemas.DocumentCreate):
    doc = models.Document(**doc.dict())
    db.add(doc)
    db.commit()
    db.refresh(doc)
    db.close()
    return doc


def get_documents(db: Session, team: str):
    docs = db.query(models.Document).filter(models.Document.team == team).all()
    db.close()
    return docs