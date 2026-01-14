import pandas as pd
import sqlite3
from connect_to_db import connect_to_db


def load_to_excel(data, filename="shopify_products.xlsx"):
    df = pd.DataFrame(data)
    no_errors = df.loc[~df["needs_fixing"]]
    errors = df.loc[df["needs_fixing"]]
    print(
        f"Writing {len(no_errors)} valid records and {len(errors)} records with errors to {filename}"
    )
    with pd.ExcelWriter(filename) as writer:
        no_errors.to_excel(writer, index=False, sheet_name="Products")
        errors.to_excel(writer, index=False, sheet_name="Errors")

    print(f"Data successfully loaded to {filename}\n")


def load_to_sql(data, db_name="shopify_products.db"):
    connection = sqlite3.connect(db_name)
    df = pd.DataFrame(data)

    # Ensure IDs are strings to prevent matching issues
    df["id"] = df["id"].astype(str)

    cols_to_drop = ["fingerprint_old"]
    df = df.drop(columns=cols_to_drop, errors="ignore")

    no_errors = df.loc[~df["needs_fixing"]]
    errors = df.loc[df["needs_fixing"]]

    def upsert_table(target_df, table_name):
        if target_df.empty:
            return

        # Create table with PRIMARY KEY if it doesn't exist
        if (
            table_name
            not in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table';"
            ).fetchall()
        ):
            cols_def = ", ".join(
                [f'"{col}" TEXT' for col in target_df.columns if col != "id"]
            )
            connection.execute(
                f"CREATE TABLE {table_name} (id TEXT PRIMARY KEY, {cols_def})"
            )
            connection.commit()

        # 1. Load data to a temporary staging table
        target_df.to_sql(
            f"temp_{table_name}", connection, if_exists="replace", index=False
        )

        # 2. Use SQLite's 'INSERT OR REPLACE' logic
        # This matches on the PRIMARY KEY.
        # Note: This requires the table to have 'id' set as a PRIMARY KEY.
        cols = ", ".join(target_df.columns)
        query = f"""
            INSERT INTO {table_name} ({cols})
            SELECT {cols} FROM temp_{table_name}
            ON CONFLICT(id) DO UPDATE SET 
            {", ".join([f"{c}=excluded.{c}" for c in target_df.columns if c != "id"])}
        """

        try:
            connection.execute(query)
            connection.execute(f"DROP TABLE temp_{table_name}")
            connection.commit()
        except sqlite3.OperationalError:
            # Fallback: If table doesn't exist yet, create it from the dataframe
            target_df.to_sql(table_name, connection, if_exists="append", index=False)
            # Ideally run: connection.execute(f"CREATE UNIQUE INDEX idx_{table_name}_id ON {table_name}(id)")
            connection.commit()

    # Apply the upsert logic to both tables
    upsert_table(no_errors, "products")
    upsert_table(errors, "errors")

    print(f"Loaded {len(no_errors)} to products and {len(errors)} to errors.\n")
    connection.close()


def check_db(field):
    conn = connect_to_db()

    # Run a simple query to check the connection
    condition = ""
    if field == "total_inventory":
        condition = f"{field} < 10"
    elif field == "price_missing":
        condition = f"{field} = 1"
    elif field == "vendor":
        condition = f"{field} = 'Unknown'"
    query = pd.read_sql_query(
        f"SELECT id, title, {field} FROM errors WHERE {condition};", conn
    )

    pretty_field = field.replace("_", " ").title()
    if field == "total_inventory":
        print(
            f"Database connection successful. Here is a summary of products with {pretty_field} less than 10:"
        )
    elif field == "price_missing":
        print(
            f"Database connection successful. Here is a summary of products with the {pretty_field}:"
        )
    elif field == "vendor":
        print(
            f"Database connection successful. Here is a summary of products with missing {pretty_field}:"
        )
    if query.size == 0:
        print(f"No products found with the specified condition for {pretty_field}.")
    else:
        print(query)
        print("\n")

    conn.close()
