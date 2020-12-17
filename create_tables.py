"""Creates the sparkifydb database.

Run this script to create the sparkifydb database along with its fact and
dimension tables. If the tables already exist, it will drop and replace
them.
"""

from typing import Tuple
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

PGCursor: psycopg2.extensions.cursor = psycopg2.extensions.cursor
PGConnection: psycopg2.extensions.connection = psycopg2.extensions.connection


def create_database() -> Tuple[PGCursor, PGConnection]:
    """Creates the sparkifydb database.

    Returns:
        A tuple containing:
            [0] a cursor to the database
            [1] a connection to the database
    """

    # connect to default database
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute(
        "CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()

    # connect to sparkify database
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    return cur, conn


def drop_tables(cur, conn) -> None:
    """Drops the fact and dimension tables in the sparkifydb database."""

    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn) -> None:
    """Creates the fact and dimension tables in sparkifydb database."""

    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main() -> None:
    """Creates a fresh sparkifydb database with empty tables."""
    cur, conn = create_database()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
