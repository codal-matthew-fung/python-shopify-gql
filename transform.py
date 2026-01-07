import pandas as pd
import numpy as np

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

    # Add Flag for Needing Fixing
    new_df = df.assign(needs_fixing=np.where((df['vendor'].isnull() | df['descriptionHtml'].isnull() | df['descriptionHtml'].str.strip().eq('')) | ('Test' in df['title']), True, False))

    return new_df
