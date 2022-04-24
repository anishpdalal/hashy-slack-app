from setuptools import find_packages, setup

setup(
    name="core",
    version="0.1.0",
    packages=find_packages(include=["core", "core"]),
    python_requires=">=3.8",
    install_requires=[
        "psycopg2-binary",
        "cryptography",
        "pydantic",
        "SQLAlchemy",
        "sqlalchemy-utils",
        "google-api-python-client",
        "google-auth",
        "google-auth-oauthlib",
        "google-auth-httplib2",
        "slack-sdk",
        "pdfminer.six",
        "requests"
    ]
)