from shopify_client import ShopifyClient

client = ShopifyClient()

test_query = """
{
  shop {
    name
    primaryDomain {
      url
    }
  }
  products(first: 3) {
    nodes {
      id
      title
    }
  }
}
"""

data = client.execute_query(test_query)
print(f"Connected to: {data['data']['shop']['name']}")

products = data['data']['products']['nodes']
print("First 3 Products:")
for product in products:
    print(f"- {product['title']} (ID: {product['id']})")