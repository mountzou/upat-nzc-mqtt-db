"""PostgreSQL connection configuration shared by API readers.

Keep the existing startup environment, cursor and Athens session policy.
This module does not create a connection until a reader requests one.
"""
import os

import psycopg2
from psycopg2.extras import RealDictCursor

from monitoring.config import APP_TIMEZONE_NAME

DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_PORT = int(os.getenv("POSTGRES_INTERNAL_PORT", "5432"))
DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")


def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        cursor_factory=RealDictCursor,
        options=f"-c timezone={APP_TIMEZONE_NAME}",
    )
