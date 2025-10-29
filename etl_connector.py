import os
import time
import logging
from datetime import datetime
from typing import List, Dict, Any

import requests
from dotenv import load_dotenv
from pymongo import MongoClient, errors

# --- Configuration & env ---
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")  # example: mongodb://localhost:27017
MONGO_DB = os.getenv("MONGO_DB", "ssn_blocklist")
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "15"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))
BACKOFF_FACTOR = float(os.getenv("BACKOFF_FACTOR", "1.5"))

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
logger = logging.getLogger("blocklist_etl")

# --- Endpoints to cover (based on blocklist.de export page) ---
LIST_ENDPOINTS = {
    "ssh": "https://lists.blocklist.de/lists/ssh.txt",
    "mail": "https://lists.blocklist.de/lists/mail.txt",
    "apache": "https://lists.blocklist.de/lists/apache.txt",
    "imap": "https://lists.blocklist.de/lists/imap.txt",
    "ftp": "https://lists.blocklist.de/lists/ftp.txt",
    "bots": "https://lists.blocklist.de/lists/bots.txt",
}

# --- Utilities: HTTP with retry/backoff ---

def safe_get(url: str) -> requests.Response:
    """GET with retries, backoff, and basic error handling."""
    session = requests.Session()
    attempt = 0
    while True:
        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT)
        except requests.RequestException as e:
            attempt += 1
            if attempt > MAX_RETRIES:
                logger.error("Max retries reached for %s: %s", url, e)
                raise
            sleep_for = BACKOFF_FACTOR ** attempt
            logger.warning("Request error, retrying in %.1fs (attempt %d/%d): %s", sleep_for, attempt, MAX_RETRIES, e)
            time.sleep(sleep_for)
            continue

        if resp.status_code == 200:
            return resp
        elif 500 <= resp.status_code < 600:
            attempt += 1
            if attempt > MAX_RETRIES:
                logger.error("Server error %d at %s", resp.status_code, url)
                resp.raise_for_status()
            sleep_for = BACKOFF_FACTOR ** attempt
            logger.warning("Server error %d — retrying in %.1fs", resp.status_code, sleep_for)
            time.sleep(sleep_for)
            continue
        else:
            logger.error("HTTP error %d for %s", resp.status_code, url)
            resp.raise_for_status()


def parse_ip_list(text: str, service: str) -> List[Dict[str, Any]]:
    """Parse plain-text IP list into MongoDB documents."""
    docs = []
    now = datetime.utcnow()
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        token = line.split()[0]
        docs.append({
            "ip": token,
            "service": service,
            "source": "blocklist.de/lists",
            "fetched_at": now,
        })
    return docs


def get_mongo_collection(connector_name: str):
    if not MONGO_URI:
        raise RuntimeError("MONGO_URI not set in environment (.env)")
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    return db[f"{connector_name}_raw"]


def safe_insert_many(collection, docs: List[Dict[str, Any]]):
    if not docs:
        logger.info("No documents to insert for %s", collection.name)
        return 0
    try:
        res = collection.insert_many(docs, ordered=False)
        inserted = len(res.inserted_ids)
        logger.info("Inserted %d documents into %s", inserted, collection.name)
        return inserted
    except errors.BulkWriteError as bwe:
        inserted = bwe.details.get("nInserted", 0)
        logger.warning("Bulk write error: inserted %d before failure. Error: %s", inserted, str(bwe))
        return inserted


def run_lists_connector():
    collection = get_mongo_collection("blocklist_lists")
    total = 0
    for service, url in LIST_ENDPOINTS.items():
        logger.info("Fetching list for '%s' from %s", service, url)
        try:
            resp = safe_get(url)
        except Exception as e:
            logger.error("Failed to fetch %s: %s", url, e)
            continue
        docs = parse_ip_list(resp.text, service)
        inserted = safe_insert_many(collection, docs)
        total += inserted
        time.sleep(1)
    logger.info("Total inserted from lists: %d", total)


def main():
    logger.info("Starting ETL connector: lists only")
    run_lists_connector()
    logger.info("ETL finished.")


if __name__ == "__main__":
    main()