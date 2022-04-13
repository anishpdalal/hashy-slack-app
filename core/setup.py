from setuptools import setup, find_packages

setup(
    name="core",
    version="0.1.0",
    packages=find_packages(where="core"),
    package_dir={"": "core"},
    python_requires=">=3.8",
    install_requires=[
        "psycopg2-binary",
        "cryptography",
        "pydantic",
        "SQLAlchemy",
        "sqlalchemy-utils",
    ]
)