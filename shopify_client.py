import os
import requests
from dotenv import load_dotenv
from typing import Dict, Any

# Load environment variables from .env file
load_dotenv()


class ShopifyClient:
    def __init__(self):
        self.shop_name = os.getenv("SHOPIFY_STORE_NAME")
        self.token = os.getenv("SHOPIFY_ACCESS_TOKEN")
        self.api_version = os.getenv("SHOPIFY_ADMIN_API_VERSION", "2024-01")
        self.url = f"https://{self.shop_name}.myshopify.com/admin/api/{self.api_version}/graphql.json"
        
        # Ensure THIS line is indented exactly like the ones above it
        self.session = requests.Session()
        
        self.session.headers.update({
            "X-Shopify-Access-Token": self.token,
            "Content-Type": "application/json"
        })

    def execute_query(self, query: str, variables: Dict[str, Any] = None) -> Dict[str, Any]:
        """Sends a GQL query to Shopify and returns the JSON response."""
        response = self.session.post(
            self.url, 
            json={'query': query, 'variables': variables}
        )
        
        # Raise an error for 4xx or 5xx responses automatically
        response.raise_for_status()
        
        return response.json()