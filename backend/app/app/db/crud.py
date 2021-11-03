from sqlalchemy.orm import Session

from . import models, schemas


def get_query(db: Session, query_id: int):
    return db.query(models.Query).filter(models.Query.id == query_id).first()


def get_queries(db: Session, skip: int = 0, limit: int = 100):
    return db.query(models.Query).offset(skip).limit(limit).all()


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