import sqlite3
import pandas as pd


def connect_to_db():
    conn = sqlite3.connect("shopify_products.db")
    if conn is None:
        print("Failed to connect to the database.")
        return
    check_table_exists = """
    SELECT name
    FROM sqlite_master
    WHERE type = 'table' AND name = 'errors';
    """
    tables = pd.read_sql_query(check_table_exists, conn)
    if tables.empty:
        print("Table 'errors' does not exist in the database.")
        return
    return conn
