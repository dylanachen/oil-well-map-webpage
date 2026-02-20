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


