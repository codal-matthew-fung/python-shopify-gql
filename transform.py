import pandas as pd

def transform_products(product_list):
    # Convert list of products to DataFrame
    df = pd.DataFrame(product_list)

    # Clean up IDs
    df['id'] = df['id'].str.split('/').str[-1]

    # Handle Missing Values

    # If Vendor is missing, fill with 'Unknown'
    df['vendor'] = df['vendor'].fillna('Unknown')
    
    # Add a transform timestamp
    df['processed_at'] = pd.Timestamp.now()

    return df
