from sqlalchemy import Column, Integer, Text, DateTime
from sqlalchemy.sql import func

from base_class import Base


class Query(Base):
    id = Column(Integer, primary_key=True, index=True)
    text = Column(Text, nullable=False)
    result = Column(Text, nullable=False)
    evidence = Column(Text, nullable=True)
    time_created = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    time_updated = Column(DateTime(timezone=True), onupdate=func.now())
