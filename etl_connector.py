"""
ETL Connector for Blocklist.de (Validated Version)
Author: K. Keerthana
Roll No: 3122225001060
Description:
This script extracts data from multiple blocklist.de endpoints,
validates responses, and loads structured IP data into MongoDB.
It includes retry logic, rate limit handling, and MongoDB insertion validation.
"""

import os
import time
import logging
from datetime import datetime
from typing import List, Dict, Any

import requests
from dotenv import load_dotenv
from pymongo import MongoClient, errors
import ipaddress

# ------------------ Load Environment Variables ------------------
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB", "ssn_blocklist")
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "15"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))
BACKOFF_FACTOR = float(os.getenv("BACKOFF_FACTOR", "1.5"))

# ------------------ Logging Setup ------------------
logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
logger = logging.getLogger("blocklist_etl")

# ------------------ Endpoints ------------------
LIST_ENDPOINTS = {
    "ssh": "https://lists.blocklist.de/lists/ssh.txt",
    "mail": "https://lists.blocklist.de/lists/mail.txt",
    "apache": "https://lists.blocklist.de/lists/apache.txt",
    "imap": "https://lists.blocklist.de/lists/imap.txt",
    "ftp": "https://lists.blocklist.de/lists/ftp.txt",
    "bots": "https://lists.blocklist.de/lists/bots.txt",
}

# ------------------ HTTP GET with Retry, Backoff & Rate Limit Handling ------------------
def safe_get(url: str) -> requests.Response:
    """Fetch data with retries, backoff, and rate-limit handling."""
    session = requests.Session()
    attempt = 0

    while attempt < MAX_RETRIES:
        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT)

            # Check for HTTP 429 (rate limit)
            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", "10"))
                logger.warning("Rate limit hit (429). Retrying after %ds", retry_after)
                time.sleep(retry_after)
                attempt += 1
                continue

            # Successful response
            if resp.status_code == 200:
                if not resp.text.strip():
                    raise ValueError("Empty payload received.")
                return resp

            # Server-side errors (5xx)
            elif 500 <= resp.status_code < 600:
                attempt += 1
                sleep_for = BACKOFF_FACTOR ** attempt
                logger.warning("Server error %d — retrying in %.1fs", resp.status_code, sleep_for)
                time.sleep(sleep_for)
                continue

            # Other HTTP errors
            else:
                logger.error("HTTP error %d for %s", resp.status_code, url)
                resp.raise_for_status()

        except requests.RequestException as e:
            attempt += 1
            if attempt >= MAX_RETRIES:
                logger.error("Max retries reached for %s: %s", url, e)
                raise
            sleep_for = BACKOFF_FACTOR ** attempt
            logger.warning("Request failed, retrying in %.1fs (attempt %d/%d): %s",
                           sleep_for, attempt, MAX_RETRIES, e)
            time.sleep(sleep_for)

    raise RuntimeError(f"Failed to fetch {url} after {MAX_RETRIES} retries")

# ------------------ Parse IP List ------------------
def parse_ip_list(text: str, service: str) -> List[Dict[str, Any]]:
    """Convert plain text IPs into structured MongoDB documents with validation."""
    docs = []
    now = datetime.utcnow()

    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        token = line.split()[0]

        # Validate if the token is a valid IP address
        try:
            ipaddress.ip_address(token)
        except ValueError:
            logger.warning("Invalid IP '%s' skipped from %s", token, service)
            continue

        docs.append({
            "ip": token,
            "service": service,
            "source": "blocklist.de/lists",
            "fetched_at": now,
        })
    return docs

# ------------------ MongoDB Helpers ------------------
def get_mongo_collection(connector_name: str):
    """Connect to MongoDB and return collection."""
    if not MONGO_URI:
        raise RuntimeError("MONGO_URI not set in environment (.env)")
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    return db[f"{connector_name}_raw"]

def safe_insert_many(collection, docs: List[Dict[str, Any]]):
    """Insert multiple documents safely with error handling."""
    if not docs:
        logger.info("No documents to insert for %s", collection.name)
        return 0
    try:
        res = collection.insert_many(docs, ordered=False)
        inserted = len(res.inserted_ids)
        logger.info("✅ Inserted %d valid IP documents into %s", inserted, collection.name)
        return inserted
    except errors.BulkWriteError as bwe:
        inserted = bwe.details.get("nInserted", 0)
        logger.warning("Bulk write error: inserted %d before failure. Error: %s", inserted, str(bwe))
        return inserted
    except Exception as e:
        logger.error("MongoDB insert failed: %s", e)
        return 0

# ------------------ Main ETL Workflow ------------------
def run_lists_connector():
    """Fetch, validate, and insert all blocklist services."""
    collection = get_mongo_collection("blocklist_lists")
    total = 0

    for service, url in LIST_ENDPOINTS.items():
        logger.info("🔹 Fetching list for '%s' from %s", service, url)
        try:
            resp = safe_get(url)
        except Exception as e:
            logger.error("Failed to fetch %s: %s", url, e)
            continue

        docs = parse_ip_list(resp.text, service)
        inserted = safe_insert_many(collection, docs)
        total += inserted
        time.sleep(1)  # polite delay between API calls

    logger.info("🎯 Total valid IPs inserted from all lists: %d", total)

# ------------------ Main Entry ------------------
def main():
    logger.info("🚀 Starting ETL connector for blocklist.de lists")
    run_lists_connector()
    logger.info("✅ ETL process completed successfully.")

if __name__ == "__main__":
    main()
