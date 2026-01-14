import pandas as pd
import numpy as np
import hashlib
import json
from connect_to_db import connect_to_db


def check_low_inventory(product):
    total_inventory = 0
    for variant in product["variants"]["edges"]:
        inventory_quantity = variant["node"]["inventoryQuantity"]
        if inventory_quantity is not None:
            total_inventory += inventory_quantity
    return total_inventory


def check_missing_price(variant):
    price = variant.get("price")
    if price is None:
        return True
    return False


def hash_string(s):
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def check_fingerprint(row):
    old_fingerprint = row["fingerprint"]
    if not old_fingerprint:
        return

    if isinstance(old_fingerprint, str):
        old_fingerprint = json.loads(old_fingerprint)

    current_hashed_title = hash_string(row["title"])
    current_hashed_description = hash_string(
        row["descriptionHtml"] if row["descriptionHtml"] else ""
    )
    current_hashed_vendor = hash_string(row["vendor"] if row["vendor"] else "")

    if current_hashed_title != old_fingerprint.get("title_hash"):
        print(f"\nTitle has changed to {row['title']}.")
    if current_hashed_description != old_fingerprint.get("description_hash"):
        print(
            f"Description has changed for product ID {row['id']} to {row['descriptionHtml']}."
        )
    if current_hashed_vendor != old_fingerprint.get("vendor_hash"):
        print(f"Vendor has changed to {row['vendor']} for product ID {row['id']}.")


def create_fingerprint_dict(row):
    return {
        "title_hash": hash_string(row["title"]),
        "description_hash": hash_string(
            row["descriptionHtml"] if row["descriptionHtml"] else ""
        ),
        "vendor_hash": hash_string(row["vendor"] if row["vendor"] else ""),
    }


def transform_products(product_list):
    # Convert list of products to DataFrame
    df = pd.DataFrame(product_list)

    # Clean up IDs
    df["id"] = df["id"].str.split("/").str[-1].astype(str).str.strip()
    # Handle Missing Values

    # If Vendor is missing, fill with 'Unknown'
    df["vendor"] = df["vendor"].fillna("Unknown")

    # Add a transform timestamp
    df["processed_at"] = pd.Timestamp.now()

    # Check Inventory for Each Product
    df["total_inventory"] = df.apply(check_low_inventory, axis=1)

    df["price_missing"] = df["variants"].apply(
        lambda x: any(check_missing_price(variant["node"]) for variant in x["edges"])
    )
    df["vendor_missing"] = (df["vendor"].eq("Unknown")) | (df["vendor"].eq("Anon"))

    # Convert Dict to String for Variants
    df["variants"] = df["variants"].apply(lambda x: json.dumps(x))

    # Add Flag for Needing Fixing
    new_df = df.assign(
        needs_fixing=np.where(
            (
                df["vendor"].eq("Unknown")
                | df["descriptionHtml"].isnull()
                | df["descriptionHtml"].str.strip().eq("")
            )
            | ("Test" in df["title"])
            | (df["total_inventory"] < 10)
            | (df["price_missing"]),
            True,
            False,
        )
    )

    # Hash Title/Description/Price for Dedupe (simple hash for demonstration)

    new_df["fingerprint"] = new_df.apply(create_fingerprint_dict, axis=1).apply(
        lambda x: json.dumps(x)
    )

    conn = connect_to_db()
    if conn:
        ids = new_df["id"].tolist()
        id_string = ", ".join([f"'{i}'" for i in ids])

        query = f"SELECT id, fingerprint FROM products WHERE id IN ({id_string})"
        fingerprints_df = pd.read_sql(query, conn)
        fingerprints_df["id"] = fingerprints_df["id"].astype(str).str.strip()

        conn.close()

        print(fingerprints_df)

        print(f"new_df ID type: {new_df['id'].dtype}")
        print(f"fingerprints_df ID type: {fingerprints_df['id'].dtype}")

        print("Sample new_df IDs:", new_df["id"].head(3).tolist())
        print("Sample fingerprints_df IDs:", fingerprints_df["id"].head(3).tolist())

        new_df = new_df.merge(
            fingerprints_df, on="id", how="left", suffixes=("", "_old")
        )

        new_df.apply(
            lambda row: check_fingerprint(row)
            if pd.notnull(row["fingerprint"])
            else None,
            axis=1,
        )

    return new_df
