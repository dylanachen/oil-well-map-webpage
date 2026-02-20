'''
Scraping additional well information from DrillingEdge.com
and appending to existing entries in the SQLite database.
'''

import argparse
import re
import sqlite3
import time
import logging
import requests
from pathlib import Path
from bs4 import BeautifulSoup
from urllib.parse import quote, urljoin

# Establishing constant vars and info
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

# New cols that will be added to the existing DB entries
NEW_COLUMNS = [
    ("well_status", "TEXT", "N/A"),
    ("well_type", "TEXT", "N/A"),
    ("closest_city", "TEXT", "N/A"),
    ("barrels_oil_produced", "TEXT", "N/A"),
    ("mcf_gas_produced", "TEXT", "N/A"),
    ("drillingedge_url", "TEXT", None),
]

BASE_URL = "https://www.drillingedge.com"
STATE = "north-dakota" # all of our wells in the pdf files are from ND counties
DEFAULT_DB = "oil_wells.db"
DEFAULT_DELAY = 1.0 # seconds between requests to avoid overwhelming server
REQUEST_TIMEOUT = 10 # seconds before a request is considered failed
MAX_RETRIES = 3 # max number of retries for failed requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S"
)

log = logging.getLogger(__name__)


# Database functions to open connection, get well info, and update entries
def ensure_columns(conn):
    # Adding the new info cols to the DB if they don't already exist
    cursor = conn.execute("PRAGMA table_info(wells)")

    # Get existing column names to avoid duplicating cols
    existing_cols = {row[1] for row in cursor.fetchall()}
    for col_name, col_type, default in NEW_COLUMNS:
        if col_name not in existing_cols:
            default_clause = f" DEFAULT '{default}'" if default is not None else ""
            conn.execute(
                f"ALTER TABLE wells ADD COLUMN {col_name} {col_type}{default_clause}"
            )
            # Tracking which cols are added
            log.info("Added column: %s", col_name)

    conn.commit()

def get_wells(conn):
    # Get all wells from the DB to update with the new info from drillingedge
    # this is just the info we need for creating the URL per well
    cursor = conn.execute(
        """SELECT id, api_number, well_name, county
        FROM wells
        WHERE
            api_number IS NOT NULL 
            and well_name IS NOT NULL
            and county IS NOT NULL
            and (drillingedge_url IS NULL OR drillingedge_url = '')
        ORDER BY id"""
    )

    # Convert the results to a list of dicts
    cols = [desc[0] for desc in cursor.description]
    return [dict(zip(cols, row)) for row in cursor.fetchall()]

def update_well(conn, well_id, data):
    # Adding scraped fields back to the well entry
    conn.execute(
        """UPDATE wells SET
            well_status = ?,
            well_type = ?,
            closest_city = ?,
            barrels_oil_produced = ?,
            mcf_gas_produced = ?,
            drillingedge_url = ?
        WHERE id = ?""",
        (
            data.get("well_status", "N/A"),
            data.get("well_type", "N/A"),
            data.get("closest_city", "N/A"),
            data.get("barrels_oil_produced", "0"),
            data.get("mcf_gas_produced", "0"),
            data.get("drillingedge_url", "N/A"),
            well_id
        )
    )

    conn.commit()


