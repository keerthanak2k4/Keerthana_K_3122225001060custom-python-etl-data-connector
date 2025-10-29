# Blocklist.de ETL Connector

**Student:** Keerthana K <br>
**Register No:** 3122225001060

## 1. Overview

This project implements a **Python-based ETL (Extract, Transform, Load) connector** that ingests public **Blocklist.de IP blacklist feeds**, transforms the data into structured documents, and loads them into a **MongoDB** database.

Blocklist.de provides public lists of IPs that are actively engaging in suspicious or malicious activity (e.g., SSH attacks, brute force attempts, botnet activity). This connector fetches these feeds periodically, handles errors and retries gracefully, and stores the IPs in collections for further analysis or security automation.

## 2. Features

- **Multiple Feed Support:** Supports various Blocklist.de IP list endpoints (e.g., SSH, Mail, Apache, Bots, etc.)  
- **Retry & Backoff:** Robust HTTP fetching with exponential backoff for transient network errors  
- **MongoDB Integration:** Inserts cleaned documents into a MongoDB collection per connector  
- **Timestamps & Metadata:** Adds `fetched_at` timestamps and source metadata to each document  
- **Error Handling:** Logs network errors, server errors, and bulk write issues with clear messages  
- **Modular Structure:** Clean separation of ETL steps (Extract, Transform, Load) for maintainability

## 3. Project Structure

```

blocklist_etl/
├── etl_connector.py        # Main ETL connector script
├── requirements.txt        # Python dependencies
├── .env.example            # Environment variable template
├── .gitignore              # Ignore secrets & venv
└── README.md               # Project documentation
````

## 4. Setup Instructions

### 4.1 Install Dependencies
```bash
pip install -r requirements.txt
```

### 4.2 Create Environment File
Create a `.env` file with the following content:

```
MONGO_URI=mongodb://localhost:27017
DB_NAME=threat_feeds
```

### 4.3 Run the ETL Connector

```bash
python etl_connector.py
```

The script will:

* Download data from all configured DShield feeds
* Parse and transform the data
* Insert the data into the corresponding MongoDB collections

## 5. Endpoints Covered

The script connects to the following **Blocklist.de IP list endpoints**:

| Service | URL                                                                                        |
| ------- | ------------------------------------------------------------------------------------------ |
| SSH     | [https://lists.blocklist.de/lists/ssh.txt](https://lists.blocklist.de/lists/ssh.txt)       |
| Mail    | [https://lists.blocklist.de/lists/mail.txt](https://lists.blocklist.de/lists/mail.txt)     |
| Apache  | [https://lists.blocklist.de/lists/apache.txt](https://lists.blocklist.de/lists/apache.txt) |
| IMAP    | [https://lists.blocklist.de/lists/imap.txt](https://lists.blocklist.de/lists/imap.txt)     |
| FTP     | [https://lists.blocklist.de/lists/ftp.txt](https://lists.blocklist.de/lists/ftp.txt)       |
| Bots    | [https://lists.blocklist.de/lists/bots.txt](https://lists.blocklist.de/lists/bots.txt)     |

Each endpoint returns a plain-text list of suspicious IP addresses. These are parsed and stored in MongoDB along with service metadata and timestamps.

## 6. How It Works

### 1. **Extract**

* Uses `requests` to download the plaintext IP lists from each Blocklist.de endpoint.
* Implements retry logic with exponential backoff on network and server errors.

### 2. **Transform**

* Parses the raw text line by line.
* Filters out comments and empty lines.
* Structures each IP into a Python dict with fields:

  ```json
  {
    "ip": "185.224.128.17",
    "count1": 475183,
    "count2": 9728,
    "first_seen": "2022-11-18",
    "last_seen": "2025-10-15",
    "source": "ipsascii",
    "fetched_at": {
      "$date": "2025-10-15T15:29:16.218Z"
    }
  }
  
  ```




### 3. **Load**

* Connects to MongoDB using `pymongo`.
* Inserts the parsed documents into a collection. 
