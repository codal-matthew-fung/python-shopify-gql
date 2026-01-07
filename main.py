from shopify_client import ShopifyClient
from transform import transform_products
from load import load_to_excel
import json

def extract_all_products():
    client = ShopifyClient()

    products_query = """
    query GetProducts($cursor: String) {
    products(first: 50, after: $cursor) {
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
        variables = {"cursor": cursor}
        response = client.execute_query(products_query, variables)

        if not response:
            break
        
        data = response['data']['products']
        nodes = [edge['node'] for edge in data['edges']]

        all_products.extend(nodes)

        pageInfo = data['pageInfo']
        hasNextPage = pageInfo['hasNextPage']
        cursor = pageInfo['endCursor']

        print(f"Fetched {len(all_products)} products so far...")

    return all_products


    
def run_etl():
    product_list = extract_all_products()

    print(f"Total products extracted: {len(product_list)}")

    df = transform_products(product_list)
    
    load_to_excel(df, "shopify_products.xlsx")

    print("ETL process completed successfully.")

if __name__ == "__main__":
    run_etl()
