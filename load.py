import pandas as pd

def load_to_excel(data, filename="shopify_products.xlsx"):
    df = pd.DataFrame(data)
    pd.set_option('display.max_rows', 1000)
    print(df)
    no_errors = df.loc[df['needs_fixing'] == False]
    errors = df.loc[df['needs_fixing'] == True]
    print(f"Writing {len(no_errors)} valid records and {len(errors)} records with errors to {filename}")
    with pd.ExcelWriter(filename) as writer:
        no_errors.to_excel(writer, index=False, sheet_name="Products")
        errors.to_excel(writer, index=False, sheet_name="Errors")

    print(f"Data successfully loaded to {filename}")