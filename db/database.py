import os

from sqlalchemy import create_engine

from sqlalchemy.orm import sessionmaker

user = os.environ["POSTGRES_USER"]
password = os.environ["POSTGRES_PASSWORD"]
host = os.environ["POSTGRES_HOST"]
db = os.environ["POSTGRES_DB"]
port = os.environ["POSTGRES_PORT"]


SQLALCHEMY_DATABASE_URL = f"postgresql://{user}:{password}@{host}:{port}/{db}"

engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
