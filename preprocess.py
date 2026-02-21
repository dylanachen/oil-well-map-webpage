# Preprocessing and cleaning all data in oil_wells.db SQL tables

import argparse
import re
import sqlite3
import logging
from datetime import datetime
from pathlib import Path

# Setting default database
DEFAULT_DB = "oil_wells.db"

# Max/Min Latitude and Longitude for North Dakota (found online, used for filtering out extreme coordinates)
ND_LATITUDE_MIN, ND_LATITUDE_MAX = 45.934, 48.9982
ND_LONGITUDE_MIN, ND_LONGITUDE_MAX = -104.0501, -96.5671


# Setting up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S"
)

log = logging.getLogger(__name__)


# Stripping HTML tags from text fields
def strip_html(text):
    # Removing HTML tags
    if not text or not isinstance(text, str):
        return text
    return re.sub(r"<[^>]+>", "", text)

# Removing control chars/non-printable chars, and normalizing whitespace
def strip_special_chars(text):
    if not text or not isinstance(text, str):
        return text
    text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]", "", text)
    text = re.sub(r"[\u00a0\u2000-\u200b\u202f\u205f\u3000]", " ", text)
    text = re.sub(r" {2,}", " ", text)
    return text.strip()


# Normalizing functions
# Normalizing missing values
def normalize_missing(value, field_type="text"):
    if value is None:
        return None

    # Converting empty/not applicable text strings to "N/A" and numeric to 0
    if isinstance(value, str):
        cleaned = value.strip().lower()
        if cleaned in {"", "n/a", "na", "null", "none", "-", "--"}:
            return "N/A" if field_type == "text" else 0
    
    # Converting numeric zeros to 0
    if isinstance(value, (int, float)):
        return value if value != 0 else 0

    return value

# Normalizing production columns
def normalize_production(value):
    # Converting shorthand number formats to ints
    if not value or not isinstance(value, str):
        return "N/A"
    
    value = value.strip()
    if value.lower() in {"n/a", "na", "null", "none", "-", "--"}:
        return "N/A"
    
    # Strip remaining surrounding text
    m = re.search(r"([\d,.]+)\s*k\b", value, re.IGNORECASE)
    if m:
        try:
            num = float(m.group(1).replace(",", ""))
            return str(int(num * 1000))
        except ValueError:
            pass
    
    # Strip any remaining numbers if no "k" found
    m = re.search(r"([\d,.]+)", value)
    if m:
        try:
            num = float(m.group(1).replace(",", ""))
            if num == int(num):
                return str(int(num))
            return str(num)
        except ValueError:
            pass

    return "N/A"


# Normalizing date formats to ISO
def normalize_date(date):
    if not date or not isinstance(date, str):
        return "N/A"
    
    date = date.strip()
    if date.lower() in {"n/a", "na", "null", "none", "-", "--"}:
        return "N/A"
    
    # ISO Format: YYYY-MM-DD
    if re.match(r"^\d{4}-\d{2}-\d{2}$", date):
        return date

    # Attempting common date formats
    for fmt in ("%m/%d/%Y", "%m-%d-%Y", "%m/%d/%y", "%m-%d-%y"):
        try:
            dt = datetime.strptime(date, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    
    # Handling special character formats
    cleaned = re.sub(r"['\u2019\u2032\u2033\u201C]", "/", date)
    if cleaned != date:
        for fmt in ("%m/%d/%Y", "%m/%d/%y"):
            try:
                dt = datetime.strptime(cleaned, fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue
    
    # Handling month name formats
    try:
        dt = datetime.strptime(date, "%B %d, %Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        pass
    
    return date

# Normalizing API numbers
def normalize_api_number(api):
    # API numbers should be in the format "XX-XXX-XXXXX" (2-3-5 digits)
    if not api or not isinstance(api, str):
        return None

    api = api.strip()
    if api.lower() in {"n/a", "na", "null", "none", "-", "--"}:
        return None
    
    # If in correct format
    if re.match(r"^\d{2}-\d{3}-\d{5}$", api):
        return api

    # Try to reformat from raw digits if not in correct format already
    digits = re.sub(r"[^\d]", "", api)
    if len(digits) == 10:
        return f"{digits[:2]}-{digits[2:5]}-{digits[5:10]}"
    if len(digits) == 11:
        return f"{digits[:2]}-{digits[2:5]}-{digits[5:10]}"
    
    return api


# Validating Latitude and Longitude values in-bound for North Dakota
def validate_latitude(lat):
    if lat is None or lat == 0:
        return 0
    try:
        lat = float(lat)
    except (ValueError, TypeError):
        return 0
    
    # Northern hemisphere
    if lat < 0:
        lat = abs(lat)
    
    if ND_LATITUDE_MIN <= lat <= ND_LATITUDE_MAX:
        return round(lat, 6)
    
    log.debug("Latitude out of North Dakota range: %s", lat)
    return 0

def validate_longitude(lon):
    if lon is None or lon == 0:
        return 0
    try:
        lon = float(lon)
    except (ValueError, TypeError):
        return 0
    
    # Western hemisphere
    if lon > 0:
        lon = -lon
    
    if ND_LONGITUDE_MIN <= lon <= ND_LONGITUDE_MAX:
        return round(lon, 6)
    
    log.debug("Longitude out of North Dakota range: %s", lon)
    return 0


# Separate Table Columns into respective cleaning methods
WELLS_TEXT_COLS = [
    "well_name",
    "address",
    "county",
    "field",
    "operator",
    "permit_number",
    "total_depth",
    "formation",
    "stimulation_notes",
    "well_status",
    "well_type",
    "closest_city"
]

WELLS_DATE_COLS = [
    "permit_date"
]

WELLS_NUMERIC_COLS = [
    "latitude",
    "longitude"
]

WELLS_PRODUCTION_COLS = [
    "barrels_oil_produced",
    "mcf_gas_produced"
]

# stimulation_data table
STIM_TEXT_COLS = [
    "stimulated_formation",
    "volume_units",
    "type_treatment",
    "acid_pct",
    "details"
]

STIM_DATE_COLS = [
    "date_stimulated"
]

STIM_NUMERIC_COLS = [
    "top_ft",
    "bottom_ft",
    "stimulation_stages",
    "volume",
    "lbs_proppant",
    "max_treatment_pressure_psi",
    "max_treatment_rate"
]


# Get existing column names function
def get_column_names(conn, table_name):
    cursor = conn.execute(f"PRAGMA table_info({table_name})")
    return [row[1] for row in cursor.fetchall()]


# Cleaning Functions
# Clean wells table function
def clean_wells(conn, dry_run=False):
    existing_cols = set(get_column_names(conn, "wells"))
    rows = conn.execute("SELECT * FROM wells").fetchall()
    col_names = [desc[0] for desc in conn.execute("SELECT * FROM wells").description]

    updates = 0
    field_changes = 0

    for row in rows:
        data = dict(zip(col_names, row))
        well_id = data["id"]
        changed = {}

        # Clean text columns: strip HTML, special chars, and normalize missing values
        for col in WELLS_TEXT_COLS:
            if col not in existing_cols:
                continue
            original = data.get(col)
            cleaned = strip_html(original)
            cleaned = strip_special_chars(cleaned)
            cleaned = normalize_missing(cleaned, field_type="text")
            if cleaned != original:
                changed[col] = cleaned
        
        # Date columns: normalize date formats
        for col in WELLS_DATE_COLS:
            if col not in existing_cols:
                continue
            original = data.get(col)
            cleaned = normalize_date(original)
            if cleaned != original:
                changed[col] = cleaned
        
        # API Columns: normalize API number formats
        if "api_number" in existing_cols:
            original = data.get("api_number")
            cleaned = normalize_api_number(original)
            if cleaned != original:
                changed["api_number"] = cleaned
        
        # Coordinate columns: validate lat/lon values
        if "latitude" in existing_cols:
            original = data.get("latitude")
            cleaned = validate_latitude(original)
            if cleaned != original and original not in (None, 0):
                changed["latitude"] = cleaned
        if "longitude" in existing_cols:
            original = data.get("longitude")
            cleaned = validate_longitude(original)
            if cleaned != original and original not in (None, 0):
                changed["longitude"] = cleaned
        
        # Production columns: normalize numeric values and missing values
        for col in WELLS_PRODUCTION_COLS:
            if col not in existing_cols:
                continue
            original = data.get(col)
            cleaned = normalize_production(original)
            if cleaned != original:
                changed[col] = cleaned
            
        # Updating changes
        if changed:
            updates += 1
            field_changes += len(changed)

            # Only log changes in dry run mode
            if dry_run:
                log.info(
                    "[DRY RUN] Well %d (%s): would update %d fields: %s",
                    well_id,
                    data.get("well_name", "?"),
                    len(changed),
                    list(changed.keys())
                )
            # Otherwise, apply updates to the database
            else:
                set_clause = ", ".join(f"{col} = ?" for col in changed)
                values = list(changed.values()) + [well_id]
                conn.execute(f"UPDATE wells SET {set_clause} WHERE id = ?", values)

    # Committing changes if not in dry run mode
    if not dry_run:
        conn.commit()

    return updates, field_changes

# Clean stimulation_data table function
def clean_stimulation_data(conn, dry_run=False):
    existing_cols = set(get_column_names(conn, "stimulation_data"))
    rows = conn.execute("SELECT * FROM stimulation_data").fetchall()
    col_names = [desc[0] for desc in conn.execute("SELECT * FROM stimulation_data").description]

    updates = 0
    field_changes = 0

    for row in rows:
        data = dict(zip(col_names, row))
        row_id = data["id"]
        changed = {}

        # Clean text columns: strip HTML, special chars, and normalize missing values
        for col in STIM_TEXT_COLS:
            if col not in existing_cols:
                continue
            original = data.get(col)
            cleaned = strip_html(original)
            cleaned = strip_special_chars(cleaned)
            cleaned = normalize_missing(cleaned, field_type="text")
            if cleaned != original:
                changed[col] = cleaned
        
        # Date columns: normalize date formats
        for col in STIM_DATE_COLS:
            if col not in existing_cols:
                continue
            original = data.get(col)
            cleaned = normalize_date(original)
            if cleaned != original:
                changed[col] = cleaned

        # Numeric columns: normalize missing values and ensure numeric types
        for col in STIM_NUMERIC_COLS:
            if col not in existing_cols:
                continue
            original = data.get(col)
            if original is None:
                changed[col] = 0
        
        # Updating changes
        if changed:
            updates += 1
            field_changes += len(changed)

            # Only log changes in dry run mode
            if dry_run:
                log.info(
                    "[DRY RUN] Stim row %d: would update %d fields: %s",
                    row_id,
                    len(changed),
                    list(changed.keys()),
                )
            # Otherwise, apply updates to the database
            else:
                set_clause = ", ".join(f"{k} = ?" for k in changed)
                values = list(changed.values()) + [row_id]
                conn.execute(f"UPDATE stimulation_data SET {set_clause} WHERE id = ?", values)
    
    # Committing changes if not in dry run mode
    if not dry_run:
        conn.commit()
    
    return updates, field_changes


# Summary printing function
def print_summary(conn):
    log.info("")
    log.info("Database Cleaning Summary:")

    total = conn.execute("SELECT COUNT(*) FROM wells").fetchone()[0]
    with_coords = conn.execute("SELECT COUNT(*) FROM wells WHERE latitude != 0 AND longitude != 0").fetchone()[0]
    with_api = conn.execute("SELECT COUNT(*) FROM wells WHERE api_number IS NOT NULL AND api_number != ''").fetchone()[0]
    stim_rows = conn.execute("SELECT COUNT(*) FROM stimulation_data").fetchone()[0]

    # Logging initial summary stats
    log.info("Total wells: %d", total)
    log.info("With valid coords: %d", with_coords)
    log.info("With API number: %d", with_api)
    log.info("Stimulation records: %d", stim_rows)

    # Check for scraped data columns
    cols = set(get_column_names(conn, "wells"))
    if "well_status" in cols:
        scraped = conn.execute("SELECT COUNT(*) FROM wells WHERE well_status != 'N/A'").fetchone()[0]
        log.info("With scraped data: %d", scraped)
    
    # Missing value counts
    log.info("")
    log.info("Missing Values (wells)")
    check_cols = [
        "api_number",
        "well_name",
        "latitude",
        "longitude",
        "county",
        "operator",
        "well_status",
        "closest_city",
        "barrels_oil_produced",
        "mcf_gas_produced"
    ]

    # For coordinate columns, counting NULL or 0 as missing
    # For text columns, counting NULL or N/A as missing.
    for col in check_cols:
        if col not in cols:
            continue
        if col in ("latitude", "longitude"):
            missing = conn.execute(f"SELECT COUNT(*) FROM wells WHERE {col} IS NULL OR {col} = 0").fetchone()[0]
        else:
            missing = conn.execute(f"SELECT COUNT(*) FROM wells WHERE {col} IS NULL OR {col} = 'N/A'").fetchone()[0]
        log.info("  %-25s %d missing / %d total", col, missing, total)
    

# Main Function
def main():
    # Setting up command line arguments for running the script
    parser = argparse.ArgumentParser(description="Preprocess and clean the oil wells database")
    parser.add_argument("--db", default=DEFAULT_DB, help="SQLite database path")
    parser.add_argument("--dry-run", action="store_true", help="Show what would change without writing to the database",)
    args = parser.parse_args()

    # Check if DB path exists
    db_path = Path(args.db).resolve()
    if not db_path.exists():
        log.error("Database file not found: %s", db_path)
        log.error("Run pdf_extractor.py first to create the database and scrape_drillingedge.py to add additional info.")
        return 1

    # Connect to DB
    conn = sqlite3.connect(str(db_path))

    # Dry run mode logging
    if args.dry_run:
        log.info("[DRY RUN] No changes will be written.")
        log.info("")

    # Clean wells table
    log.info("Cleaning wells table.")
    well_updates, well_fields = clean_wells(conn, dry_run=args.dry_run)
    log.info("Wells: %d rows updated, %d fields changed", well_updates, well_fields)

    # Clean stimulation_data table
    log.info("Cleaning stimulation_data table.")
    stim_updates, stim_fields = clean_stimulation_data(conn, dry_run=args.dry_run)
    log.info("Stimulations: %d rows updated, %d fields changed", stim_updates, stim_fields)

    # Summary
    print_summary(conn)

    # Closing connection
    conn.close()
    log.info("")
    log.info("Preprocessing complete.")
    return 0


if __name__ == "__main__":
    exit(main())
