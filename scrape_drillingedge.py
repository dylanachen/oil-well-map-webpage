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


# URL Construction and scraping functions
def make_url_compatible(text):
    # Converting well names and county names to URL compatible formats
    text = text.lower().strip()

    # Replacing non-alnum chars with hyphens and combining multiple hyphens into one
    text = re.sub(r"[^a-z0-9\-]+", "-", text)
    text = re.sub(r"-{2,}", "-", text)

    # Return without leading/trailing hyphens
    return text.strip("-")

def construct_url(api_number, well_name, county):
    # Constructing the URL for drillingedge well pages
    if not api_number:
        return None
    
    county_portion = make_url_compatible(county) + "-county" if county and county != "N/A" else None
    if not county_portion:
        return None
    
    well_name_portion = make_url_compatible(well_name) if well_name and well_name != "N/A" else None
    if not well_name_portion:
        return None
    
    # Example URL format
    # https://www.drillingedge.com/north-dakota/mckenzie-county/wells/yukon-5301-41-12t/33-053-03911
    return f"{BASE_URL}/{STATE}/{county_portion}/wells/{well_name_portion}/{api_number}"


# HTML Parsing function
def parse_well_page(html, url=None):
    # Extracting target fields from drillingedge well page
    soup = BeautifulSoup(html, "html.parser")
    result = {
        "well_status": "N/A",
        "well_type": "N/A",
        "closest_city": "N/A",
        "barrels_oil_produced": "0",
        "mcf_gas_produced": "0",
        "drillingedge_url": url or "N/A"
    }

    # All tags based on scraping from static fields on well pages

    # <p class="block_stat"><span class="dropcap">1.1 k</span> Barrels of Oil Produced in Dec 2025</p>
    # <p class="block_stat"><span class="dropcap">1.5 k</span> MCF of Gas Produced in Dec 2025</p>
    for p_tag in soup.select("p.block_stat"):
        text = p_tag.get_text(separator=" ", strip=True)
        dropcap = p_tag.select_one("span.dropcap")
        value = dropcap.get_text(strip=True) if dropcap else ""

        if "barrels of oil produced" in text.lower():
            result["barrels_oil_produced"] = value
        elif "mcf of gas produced" in text.lower():
            result["mcf_gas_produced"] = value
        
    # Well Detail Table
        # <tr>
        # <th>Well Status</th><td>Active</td>
        # <th>Well Type</th><td>Oil &amp; Gas</td>
        # <th>Township Range Section</th><td colspan="3">153 N 101 W 12</td>
        # </tr>
    for tr in soup.select("tr"):
        ths = tr.find_all("th")
        tds = tr.find_all("td")

        # Aligning the th and td pairs to extract our info
        cells = list(tr.children)
        # Filter to only tags
        cells = [cell for cell in cells if hasattr(cell, "name") and cell.name in ("th", "td")]

        pairs = {}
        i = 0
        while i < len(cells) - 1:
            if cells[i].name == "th":
                key = cells[i].get_text(strip=True).lower()
                value = cells[i + 1].get_text(strip=True)
                pairs[key] = value
                i += 2
            else:
                i += 1
        
        for key, value in pairs.items():
            if not value or value == "N/A":
                continue

            if "well status" in key:
                result["well_status"] = value
            elif "well type" in key:
                result["well_type"] = value
            elif "closest city" in key:
                result["closest_city"] = value
        
    return result


# Web Scraping function
def fetch_page(url, session):
    # Get HTML content from well page with retries
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)

            # Raise an error for bad status codes
            response.raise_for_status()

            # Otherwise, return content and url
            return response.text, response.url
        
        # If there's an error and we haven't gone past max retries, log and retry after delay
        except requests.RequestException as e:
            log.warning("Request error (attempt %d): %s", attempt + 1, e)
            if attempt < MAX_RETRIES:
                time.sleep(2)
    return None, None


# Well scraping function
def scrape_well(well, session, delay=DEFAULT_DELAY):
    # Scrape the drillingedge page for a single well and return the additional info
    id = well["id"]
    api_number = well["api_number"]
    well_name = well["well_name"]
    county = well["county"]

    if not api_number or api_number == "N/A":
        log.warning("Skipping well with missing API number (ID: %d)", id)
        return None

    # Building URl and fetching page content
    url = construct_url(api_number, well_name, county)
    html = None
    final_url = None

    if url:
        log.info("Fetching page for well ID %d at URL: %s", id, url)
        html, final_url = fetch_page(url, session)
        if not html:
            log.warning("Failed to fetch page for well ID %d at URL: %s", id, url)
            return None

    # Parsing the HTMl content to extract desired info
    data = parse_well_page(html, url=final_url or url) if html else None

    return data


# Main function to run DB interactions, scraping, parsing, and updating well entries
def main():
    # Setting up command line arguments for running the script
    parser = argparse.ArgumentParser(description="Scrape drillingedge.com for additional well info")
    parser.add_argument("--db", type=str, default=DEFAULT_DB, help="SQLite database path")
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY, help="Delay between requests in seconds (default: 1.0)")
    parser.add_argument("--max-wells", type=int, default=None, help="Maximum number of wells to process (default: all)")
    parser.add_argument("--rescrape", action="store_true", help="Rescrape wells even if they already have a drillingedge_url")
    args = parser.parse_args()

    # Check if DB path exists
    db_path = Path(args.db).resolve()
    if not db_path.exists():
        log.error("Database file not found: %s", db_path)
        log.error("Run pdf_extractor.py first to create the database.")
        return 1

    # Connect to DB and ensure new cols exist
    conn = sqlite3.connect(str(db_path))
    ensure_columns(conn)

    # Reset drillingedge_url for all wells if --rescrape is specified, so they will be reprocessed
    if args.rescrape:
        conn.execute("UPDATE wells SET drillingedge_url = NULL")
        conn.commit()
    
    # Get wells to process, limiting by max-wells
    wells = get_wells(conn)
    if args.max_wells:
        wells = wells[:args.max_wells]

    # If there are no wells to process, log and exit (tell to rescrape)
    total = len(wells)
    if total == 0:
        log.info("No wells to scrape. All wells already have drillingedge_url.")
        log.info("Use --rescrape to force reprocessing of all wells.")
        conn.close()
        return 0

    log.info("Scraping %d wells from drillingedge.com", total)

    # Loop through wells, scrape info, and update DB entries
    session = requests.Session()
    success = 0
    failed = 0

    try:
        for i, well in enumerate(wells, 1):
            log.info(
                "[%d/%d] %s (API: %s)",
                i,
                total,
                well.get("well_name", "Unknown"),
                well.get("api_number", "N/A")
            )

            # Scrape the well page and extract info, then update the DB entry with the new info
            data = scrape_well(
                well, session, delay=args.delay
            )

            if data:
                update_well(conn, well["id"], data)
                success += 1
                log.info(
                    "Status: %s | Type: %s | City: %s | Oil: %s | Gas: %s",
                    data["well_status"],
                    data["well_type"],
                    data["closest_city"],
                    data["barrels_oil_produced"],
                    data["mcf_gas_produced"]
                )
            else:
                failed += 1
            
            # Delay requests
            if i < total:
                time.sleep(args.delay)
    
    # Allow user interruption, but log progress
    except KeyboardInterrupt:
        log.warning("Scraping interrupted by user. Progress has been saved.")
    finally:
        conn.close()
    
    log.info("")
    log.info("Scraping complete. %d / %d successful, %d failed.", success, total, failed)
    return 0


if __name__ == "__main__":
    exit(main())
        