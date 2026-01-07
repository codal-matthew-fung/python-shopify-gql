from shopify_client import ShopifyClient
from transform import transform_products
from load import load_to_excel, load_to_sql, check_db
from pathlib import Path
import json
import inquirer
from query_db import query_db
import sys
import os


def get_last_updated_timestamp():
    path = Path("watermark.json")
    last_updated = "1970-01-01T00:00:00Z"  # Default to epoch start
    if path.is_file():
        watermark = open("watermark.json", "r").read()
        if watermark:
            watermark_json = json.loads(watermark)
            if watermark_json and ("last_updated" in watermark_json):
                last_updated = watermark_json["last_updated"]
    return last_updated


def extract_all_products(last_updated="1970-01-01T00:00:00Z"):
    last_updated = get_last_updated_timestamp()

    client = ShopifyClient()

    products_query = """
    query GetProducts(
        $cursor: String
        $query: String = "updated_at:>'{last_updated}'"
    ) {
    products(first: 50, after: $cursor, reverse: true, query: $query) {
        pageInfo {
            hasNextPage
            endCursor
        }
        edges {
            node {
                id
                title
                descriptionHtml
                handle
                vendor
                updatedAt
            }
        }
    }
    }
    """

    all_products = []
    hasNextPage = True
    cursor = None

    print("Extracting products...")

    while hasNextPage:
        variables = {"cursor": cursor, "query": f"updated_at:>'{last_updated}'"}
        response = client.execute_query(products_query, variables)
        if not response or "errors" in response:
            if "errors" in response:
                print("Errors occurred while fetching products:", response["errors"])
            break

        if len(response["data"]["products"]["edges"]) == 0:
            print(
                f"No products found updated after the watermark timestamp ({last_updated})."
            )
            break

        data = response["data"]["products"]
        nodes = [edge["node"] for edge in data["edges"]]

        all_products.extend(nodes)

        pageInfo = data["pageInfo"]
        hasNextPage = pageInfo["hasNextPage"]
        cursor = pageInfo["endCursor"]

        print(f"Fetched {len(all_products)} products so far...")

    return all_products


def update_watermark(product_list):
    if not product_list:
        print("No products found to update watermark.")
        return
    print("Updating watermark...")
    sorted_products = sorted(product_list, reverse=True, key=lambda x: x["updatedAt"])
    most_recent_updated_product = sorted_products[0]
    with open("watermark.json", "w") as f:
        watermark = {"last_updated": most_recent_updated_product.get("updatedAt")}
        json.dump(watermark, f)
        print(
            f"Watermark updated, the most recent product updated at: {most_recent_updated_product.get('updatedAt')}"
        )


def run_etl():
    last_updated = get_last_updated_timestamp()
    product_list = extract_all_products(last_updated)

    if product_list is None or len(product_list) == 0:
        print(f"The watermark timestamp is: {last_updated}")
        print("No products extracted. ETL process terminated.")
        return

    print(f"Total products extracted: {len(product_list)}")

    update_watermark(product_list)

    df = transform_products(product_list)

    load_to_excel(df, "shopify_products.xlsx")

    load_to_sql(df)

    check_db()

    print("ETL process completed.")

    sys.exit(0)


if __name__ == "__main__":
    if (
        not os.getenv("SHOPIFY_STORE_NAME")
        or not os.getenv("SHOPIFY_ACCESS_TOKEN")
        or not os.getenv("SHOPIFY_ADMIN_API_VERSION")
    ):
        print(
            "Please set the SHOPIFY_STORE_NAME, SHOPIFY_ACCESS_TOKEN, and SHOPIFY_ADMIN_API_VERSION environment variables in your .env file."
        )
        sys.exit(1)

    questions = [
        inquirer.List(
            "action",
            message="What do you want to do?",
            choices=["Run ETL Process", "Query the Database", "Exit"],
        )
    ]

    answers = inquirer.prompt(questions)

    if not answers:
        print("No action selected. Exiting.")
    if answers["action"] == "Run ETL Process":
        run_etl()
    elif answers["action"] == "Query the Database":
        query_db()
