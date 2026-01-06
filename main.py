from shopify_client import ShopifyClient
from transform import transform_products
import json

def extract_all_products(data):
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
        handle
        vendor
      }
    }
  }
}
"""

product_list = extract_all_products(client)

print(f"Total products extracted: {len(product_list)}")

df = transform_products(product_list)

print(df.head())

print(f"Products by Vendor:\n{df['vendor'].value_counts()}")

    
