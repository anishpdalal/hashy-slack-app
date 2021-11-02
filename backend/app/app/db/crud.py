from sqlalchemy.orm import Session

from models import Query


def get_query(db: Session, question_id: int):
    return db.query(Query).filter(Query.id == question_id).first()


def get_queries(db: Session, skip: int = 0, limit: int = 100):
    return db.query(Query).offset(skip).limit(limit).all()
