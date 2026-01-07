import pandas as pd
import sqlite3


def load_to_excel(data, filename="shopify_products.xlsx"):
    df = pd.DataFrame(data)
    no_errors = df.loc[not df["needs_fixing"]]
    errors = df.loc[df["needs_fixing"]]
    print(
        f"Writing {len(no_errors)} valid records and {len(errors)} records with errors to {filename}"
    )
    with pd.ExcelWriter(filename) as writer:
        no_errors.to_excel(writer, index=False, sheet_name="Products")
        errors.to_excel(writer, index=False, sheet_name="Errors")

    print(f"Data successfully loaded to {filename}")


def load_to_sql(data, db_name="shopify_products.db"):
    connection = sqlite3.connect(db_name)
    df = pd.DataFrame(data)

    no_errors = df.loc[not df["needs_fixing"]]
    errors = df.loc[df["needs_fixing"]]

    no_errors.to_sql("products", connection, if_exists="replace", index=False)
    errors.to_sql("errors", connection, if_exists="replace", index=False)
    print("First 5 records loaded to 'products' table:")
    print(no_errors.head())

    print("First 5 records loaded to 'errors' table:")
    print(errors.head())

    connection.close()
    print("Data successfully loaded to SQLite database")


def check_db():
    conn = sqlite3.connect("shopify_products.db")

    if conn is None:
        print("Failed to connect to the database.")
        return
    check_table_exists = """
    SELECT name
    FROM sqlite_master
    WHERE type = 'table' AND name = 'products';
    """
    tables = pd.read_sql_query(check_table_exists, conn)
    if tables.empty:
        print("Table 'products' does not exist in the database.")
        return

    # Run a simple query to check the connection
    error_query = "SELECT id, title, needs_fixing FROM products WHERE needs_fixing = 1;"

    error_query = pd.read_sql_query(error_query, conn)
    print(
        "Database connection successful. Here is a summary of products by that need fixing:"
    )
    print(error_query)

    conn.close()
