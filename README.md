# Shopify Data Orchestrator: Python ETL Pipeline

A production-grade ETL (Extract, Transform, Load) pipeline built in Python to interface with the Shopify GraphQL Admin API. This project demonstrates advanced data engineering patterns, moving away from standard REST constraints to leverage the efficiency of GraphQL and the analytical power of Pandas.

---

## Project Overview

This pipeline automates the extraction of product data from Shopify, cleans and normalizes it for analysis, and loads it into both a relational database (SQLite) and a business-ready report (Excel). 

Unlike basic scripts, this project implements a **"Smart Sync"** system using a watermark pattern to only process data that has changed, saving API costs and execution time.

---

## Tech Stack

| Component | Technology | Purpose |
| :--- | :--- | :--- |
| **Language** | Python 3.x | Core logic and scripting. |
| **API** | Shopify GQL | High-efficiency data retrieval. |
| **Data Handling** | Pandas | Vectorized cleaning and transformation. |
| **Storage** | SQLite | Local SQL database for structured storage. |
| **Reporting** | Openpyxl | Generating formatted Excel workbooks. |
| **Environment** | Dotenv | Secure management of API credentials. |

---

## How It Works

### 1. Extract (GraphQL + Pagination)
The pipeline uses a `while` loop to navigate Shopify’s **Cursor-based Pagination**. 
* It requests batches of 50 nodes.
* It captures the `endCursor` from the `pageInfo` object.
* It passes that cursor back into the next request until `hasNextPage` is false.

### 2. Transform (Pandas Logic)
Raw nested JSON is converted into a flat **Pandas DataFrame**.
* **GID Parsing:** Strips `gid://shopify/Product/` strings to clean numeric IDs.
* **Timestamp Normalization:** Converts ISO 8601 strings into Python `datetime` objects.
* **Audit Layer:** Identifies "Action Required" items (e.g. missing description, improper titles, missing vendor).

### 3. Load (Multi-Destination)
* **SQLite:** Data is "upserted" into the `products` table for long-term storage.
* **Excel:** Generates a multi-sheet report where errors are separated into a dedicated "Audit Log" tab for business users.

---

## 🧠 Key Learnings: Transitioning from JS to Python

Coming from a JavaScript background, this project highlighted several "Pythonic" advantages:

* **Synchronous Flow:** Python’s `requests` library allows for a straightforward blocking flow that naturally respects Shopify’s "Leaky Bucket" rate limits without the complexity of `async/await` overhead.
* **Vectorization:** Using Pandas for transformation proved significantly faster than using `.map()` or `.filter()` in JS, as Pandas operates on entire columns at once via C-extensions.
* **Data Integrity:** Handling Excel generation via Python avoids the common "CSV mangling" issues (like leading zeros being stripped from SKUs) found in standard spreadsheet software.

---

## 📁 Project Structure

```text
python-training/
├── main.py             # Pipeline Orchestrator
├── shopify_client.py   # GQL API Client & Session Management
├── transform.py        # Pandas Transformation Logic
├── load.py             # Write data to Excel and SQL
├── watermark.json      # Watermark for Incremental Syncs
├── requirements.txt    # Project Dependencies
└── README.md           # Project Documentation
```