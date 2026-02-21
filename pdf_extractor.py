#!/usr/bin/env python3
# Extract well info and stimulation data from PDFs into SQLite.

import argparse
import os
import re
import sqlite3
from datetime import datetime
from pathlib import Path

try:
    import pdfplumber
except ImportError:
    print("pdfplumber not installed. Run: pip install pdfplumber")
    exit(1)

try:
    import pytesseract
    from pdf2image import convert_from_path
except ImportError:
    pytesseract = None
    convert_from_path = None

PDF_DIR = "pdfs"
DB_PATH = "oil_wells.db"


def _coord_bounds():
    # read coord bounds from env (unset = full valid range)
    lat_min = float(os.environ.get("COORD_LAT_MIN", "-90"))
    lat_max = float(os.environ.get("COORD_LAT_MAX", "90"))
    lon_abs_min = float(os.environ.get("COORD_LON_ABS_MIN", "0"))
    lon_abs_max = float(os.environ.get("COORD_LON_ABS_MAX", "180"))
    lon_deg_min = float(os.environ.get("COORD_LON_DEG_MIN", "0"))
    return lat_min, lat_max, lon_abs_min, lon_abs_max, lon_deg_min


def normalize_dms(text):
    # unify degree/min/sec symbols so regex can match
    text = re.sub(r'[\u00BA\u02DA\u00B7\u02D9]', '\u00B0', text)
    text = re.sub(r'[\u2032\u2019\u02BC\u02B9`\u00B4]', "'", text)
    text = re.sub(r'[\u2033\u201D\u02BA]', '"', text)
    text = text.replace('~', '')
    return text


def dms_to_decimal(dms_str):
    # parse dms to decimal degrees, negate for W/S
    if not dms_str or not dms_str.strip():
        return None
    s = re.sub(r'[\u00B0\u2032\u2033\u2019]', ' ', dms_str).replace('"', ' ').strip()
    parts = re.findall(r'-?\d+\.?\d*', s)
    if len(parts) < 3:
        return None
    try:
        deg, mins, secs = float(parts[0]), float(parts[1]), float(parts[2])
    except ValueError:
        return None
    if mins >= 60 or secs >= 60:
        return None
    dec = abs(deg) + mins / 60 + secs / 3600
    if deg < 0 or 'W' in dms_str.upper() or 'S' in dms_str.upper():
        dec = -dec
    return round(dec, 6)


def parse_num(s):
    # first number in string, strip commas and spaces
    if not s:
        return None
    cleaned = re.sub(r'[,\s]', '', s.strip())
    m = re.search(r'-?\d+\.?\d*', cleaned)
    if m:
        try:
            return float(m.group(0))
        except ValueError:
            return None
    return None


def clean_value(val):
    # missing -> N/A, strip html and control chars
    if val is None or (isinstance(val, str) and not val.strip()):
        return 'N/A'
    if isinstance(val, str):
        val = re.sub(r'<[^>]+>', '', val)
        val = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]', '', val)
        val = val.strip()
    return val


def normalize_date_to_iso(date_str):
    # try MM/DD and DD/MM, 2-digit year via env cutoff
    if not date_str or not isinstance(date_str, str) or not date_str.strip():
        return None
    s = re.sub(r'[^\d/\-]', '/', date_str.strip())
    parts = [p for p in s.split('/') if p.strip()]
    if len(parts) != 3:
        return None
    try:
        a, b, c = int(parts[0]), int(parts[1]), int(parts[2])
    except ValueError:
        return None
    cutoff = int(os.environ.get("DATE_2DIGIT_YEAR_CUTOFF", "50"))
    if c < 100:
        c += 2000 if c < cutoff else 1900
    fmt = os.environ.get("DATE_OUTPUT_FORMAT", "%Y-%m-%d")
    for (mo, day) in [(a, b), (b, a)]:
        if 1 <= mo <= 12 and 1 <= day <= 31:
            try:
                return datetime(c, mo, day).strftime(fmt)
            except ValueError:
                continue
    return None


def _apply_ocr_fixes(val, env_key, default):
    # apply typo:fix pairs from env
    if not val:
        return val
    s = os.environ.get(env_key, default)
    if not s:
        return val
    for part in s.split(","):
        part = part.strip()
        if ":" in part:
            typo, fix = part.split(":", 1)
            typo, fix = typo.strip(), fix.strip()
            if typo:
                val = val.replace(typo, fix)
    return val


def _is_truncated_well_name(name):
    # name ending in digit-dash (truncated line)
    if not name or len(name) < 4:
        return False
    return bool(re.search(r'\d\s*[-‐–—]\s*$', name))


def _is_rejected_well_name(name):
    # truncated, or compass token, or in reject list
    if not name:
        return False
    if _is_truncated_well_name(name):
        return True
    n = re.sub(r'\s+', '', name.lower())
    pattern = os.environ.get("WELL_NAME_REJECT_REGEX", r"^[nsew]{2,6}$")
    if pattern:
        try:
            if re.fullmatch(pattern, n):
                return True
        except re.error:
            pass
    lst = os.environ.get("WELL_NAME_REJECT_LIST")
    if lst:
        for w in lst.split(","):
            w = w.strip().lower()
            if w and n == w:
                return True
    return False


def _is_garbled_well_name(name):
    # long string with many punctuation runs (ocr noise)
    if not name or len(name) < 20:
        return False
    if len(re.findall(r'[~=.:;]\s*', name)) >= 3:
        return True
    if re.search(r'[~=]\s*[A-Z]?\s*[~=]', name):
        return True
    return False


def _sanitize_garbled_well_name(name):
    # merge single-char runs, strip junk suffixes
    if not name:
        return None
    s = re.sub(r'[~=.:;]+', ' ', name)
    s = re.sub(r'\s+', ' ', s).strip()
    s = re.sub(r'[^\w\s\-&\'#]', '', s)
    s = re.sub(r'\s+', ' ', s).strip()
    max_single_run = 6
    parts = s.split()
    merged = []
    i = 0
    while i < len(parts):
        if len(parts[i]) == 1 and (parts[i].isalpha() or parts[i].isdigit()):
            run = []
            while i < len(parts) and len(parts[i]) == 1 and (parts[i].isalpha() or parts[i].isdigit()) and len(run) < max_single_run:
                run.append(parts[i])
                i += 1
            word = ''.join(run)
            if word.isalpha() and len(word) > 1:
                word = word.capitalize()
            merged.append(word)
        else:
            merged.append(parts[i])
            i += 1
    out = []
    for p in merged:
        if out and len(p) == 2 and p.isalpha() and p.isupper() and out[-1].isalpha() and out[-1][-1].islower():
            out[-1] = out[-1] + p.lower()
        else:
            out.append(p)
    s = ' '.join(out)
    for pat in [r'\s+Wiltiams\s*.*$', r'\s+Williams\s*$', r'\s+McKenzie\s*$', r'\s+LOT\d*\s*$', r'\s+Sec\s+\d', r'\s+\d{2,3}\s+\d{2,3}\s+\d+\s*$', r'\s+_{2,}.*$']:
        s = re.sub(pat, '', s, flags=re.IGNORECASE).strip()
    if 3 < len(s) < 120 and not _is_rejected_well_name(s):
        return s
    return None


def extract_api(text):
    # search survey/permit block first to avoid commingling API
    survey = re.search(
        r'(?:Directional\s+Survey|Survey\s+(?:Report|Certification)|'
        r'Well\s+Completion|APPLICATION\s+FOR\s+PERMIT)[^\n]*((?:.*\n){0,20})',
        text, re.IGNORECASE,
    )
    regions = [survey.group(0)] if survey else []
    regions.append(text)

    patterns = [
        r'API\s*[#:\s]*(\d{2})-(\d{3})-(\d{5})(?:-\d{2}-\d{2})?',
        r'API\s*[:#]?\s*(\d{2})\s*[-]\s*(\d{3})\s*[-]\s*(\d{5})',
        r'API\s*[:#]?\s*(\d{2})\s*[-]?\s*(\d{3})\s*[-]?\s*(\d{5})',
        r'API\s+(?:No\.?|Number|JOB\s*#?)\s*[:\s]*(\d{2})-(\d{3})-(\d{5})',
        r'API\s+(?:No\.?|Number|JOB\s*#?)\s*[:\s]*(\d{2})\s+(\d{3})\s+(\d{5})\b',
        r'API\s*[:#]?\s*(\d{10,11})\b',
    ]
    for region in regions:
        for p in patterns:
            m = re.search(p, region, re.IGNORECASE)
            if m:
                if len(m.groups()) == 3:
                    return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
                raw = m.group(1)
                if len(raw) == 10:
                    return f"{raw[:2]}-{raw[2:5]}-{raw[5:10]}"
                if len(raw) == 11 and raw[:2] == '33':
                    return f"33-{raw[3:6]}-{raw[6:11]}"
                if len(raw) >= 10:
                    return f"{raw[:2]}-{raw[2:5]}-{raw[5:10]}"
                return raw

    # space-separated API e.g. 33 053 06755
    m = re.search(r'\b(33)\s+(\d{3})\s+(\d{5})\b', text)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

    m = re.search(r'(\d{2})-(\d{3})-(\d{5})\b', text)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return None


def extract_well_file_from_filename(filename):
    # W12345.pdf -> 12345
    m = re.match(r'W(\d+)\.pdf', filename, re.IGNORECASE)
    return m.group(1) if m else None


def extract_well_file_from_text(text):
    # well file # or file # from text
    m = re.search(r'Well\s*File\s*(?:#|Number)?[:\s]*(\d{4,6})', text, re.IGNORECASE)
    if m:
        return m.group(1)
    m = re.search(r'File\s*#\s*(\d{4,6})', text, re.IGNORECASE)
    return m.group(1) if m else None


def _nd_lat_ok(v):
    return 45.9 <= v <= 49.1 if v is not None else False


def _nd_lon_ok(v):
    return 96.5 <= abs(v) <= 104.1 if v is not None else False


def extract_latitude(text):
    norm = normalize_dms(text)
    lat_min, lat_max, _, _, _ = _coord_bounds()
    candidates = []

    dms_pats = [
        r'Well\s+Coordinates[^(]*\(\s*(\d+)\s*[°]\s*(\d+)\s*[\']?\s*([\d.]+)\s*["]?\s*N\s*[,\)]',
        r'Latitude\s+of\s+Well\s+Head[^\d]*(\d+)\s*[°]\s*(\d+)\s*[\']?\s*([\d.]+)\s*["]?',
        r'(?:Site\s+Position|Well\s+Position)[^\d]*Latitude\s*[:\s]\s*(\d+)\s*[°]\s*(\d+)\s*[\']?\s*([\d.]+)\s*["]?\s*N',
        r'Lat(?:itude|ittude)?\s*[:\s]\s*(\d{2})\s*[°]\s*(\d{1,2})\s*[\']?\s*([\d.]+)\s*["]?\s*N',
        r'(\d{2})\s*[°]\s*(\d{1,2})\s*[\']\s*([\d.]+)\s*["]?\s*N\b',
        r'Lat(?:itude|ittude)?\s*[:\s]\s*(\d{2})\s+(\d{1,2})\s+([\d.]+)\s*N\b',
    ]
    for p in dms_pats:
        m = re.search(p, norm, re.IGNORECASE)
        if m:
            dms = f"{m.group(1)}° {m.group(2)}' {m.group(3)}\" N"
            dec = dms_to_decimal(dms)
            if dec is not None and -90 <= dec <= 90:
                candidates.append(round(dec, 6))

    for pat in [
        r'(?:Survey\s+)?Lat(?:itude|ittude)?\s*[:\s]\s*(\d{2}\.\d{2,6})\s*(?:deg\.?\s*[NS]?)?',
        r'\bLat(?:itude|ittude)?\b[^\d\n]{0,20}(\d{2}\.\d{2,6})',
        r'Lat(?:itude|ittude)?\s*[:\s]+\s*(\d{2}\.\d{2,6})\s*[°N]?',
    ]:
        for m in re.finditer(pat, norm, re.IGNORECASE):
            try:
                v = float(m.group(1))
                if -90 <= v <= 90:
                    candidates.append(round(v, 6))
            except ValueError:
                pass

    survey = re.search(
        r'(?:Well\s+Coord|Survey|APPLICATION\s+FOR\s+PERMIT|Well\s+Completion)[^\d]{0,600}(\d{1,2}\.\d{2,6})',
        text, re.IGNORECASE | re.DOTALL,
    )
    if survey:
        try:
            v = float(survey.group(1))
            if lat_min <= v <= lat_max:
                candidates.append(round(v, 6))
        except ValueError:
            pass

    for v in candidates:
        if _nd_lat_ok(v):
            return v
    return round(candidates[0], 6) if candidates else None


def extract_longitude(text):
    norm = normalize_dms(text)
    _, _, lon_abs_min, lon_abs_max, lon_deg_min = _coord_bounds()
    candidates = []

    dms_pats = [
        r'Well\s+Coordinates[^)]*N\s*[,\)]\s*(\d+)\s*[°]\s*(\d+)\s*[\']?\s*([\d.]+)\s*["]?\s*W',
        r'Longitude\s+of\s+Well\s+Head[^\d]*(-?\d+)\s*[°]\s*(\d+)\s*[\']?\s*([\d.]+)\s*["]?\s*W?',
        r'(?:Site\s+Position|Well\s+Position)[^\d]*Longitude\s*[:\s]\s*(\d+)\s*[°]\s*(\d+)\s*[\']?\s*([\d.]+)\s*["]?\s*W',
        r'Long(?:itude)?\s*[:\s]\s*(\d{2,3})\s*[°]\s*(\d{1,2})\s*[\']?\s*([\d.]+)\s*["]?\s*W',
        r'Long(?:itude)?\s*[:\s]\s*(-?\d{2,3})\s*["\u201C]\s*(\d{1,2})\s*[\']?\s*([\d.]+)\s*["]?\s*W',
        r'(\d{2,3})\s*[°]\s*(\d{1,2})\s*[\']\s*([\d.]+)\s*["]?\s*W\b',
        r'Long(?:itude)?\s*[:\s]\s*(\d{2,3})\s+(\d{1,2})\s+([\d.]+)\s*W\b',
    ]
    for p in dms_pats:
        m = re.search(p, norm, re.IGNORECASE)
        if m:
            deg = int(str(m.group(1)).lstrip('-'))
            if deg < lon_deg_min:
                continue
            dms = f"-{deg}° {m.group(2)}' {m.group(3)}\" W"
            dec = dms_to_decimal(dms)
            if dec is not None and dec > 0:
                dec = -dec
            if dec is not None and -180 <= dec <= 180:
                candidates.append(round(dec, 6))

    for pat in [
        r'(?:Survey\s+)?\bLong(?:itude)?\b\s*[:\s]\s*(-?\d{2,3}\.\d{2,6})\s*(?:deg\.?\s*[WE]?)?',
        r'\bLong(?:itude)?\b[^\d\n]{0,20}(-?\d{2,3}\.\d{2,6})',
        r'Long(?:itude)?\s*[:\s]+\s*(-?\d{2,3}\.\d{2,6})\s*[°W]?',
    ]:
        for m in re.finditer(pat, norm, re.IGNORECASE):
            try:
                v = float(m.group(1))
                if v > 0:
                    v = -v
                if -180 <= v <= 180:
                    candidates.append(round(v, 6))
            except ValueError:
                pass

    survey = re.search(
        r'(?:Well\s+Coord|Survey|APPLICATION\s+FOR\s+PERMIT|Well\s+Completion)[^\d]{0,800}(\d{2,3}\.\d{2,6})',
        text, re.IGNORECASE | re.DOTALL,
    )
    if survey:
        try:
            v = float(survey.group(1))
            if lon_abs_min <= v <= lon_abs_max:
                candidates.append(round(-v, 6))
        except ValueError:
            pass

    for v in candidates:
        if _nd_lon_ok(v):
            return v
    return round(candidates[0], 6) if candidates else None


def extract_well_name(text, well_file_no=None):
    # by file number, then same-line label, then next line
    if well_file_no:
        m = re.search(
            rf'(?:Well\s+)?File\s*#?\s*:?\s*{re.escape(well_file_no)}\s+'
            r'([A-Za-z][A-Za-z0-9\s\-\.&\'~=.:;#]+?)'
            r'(?:\s+(?:LOT\d?|[SN][EW][SN][EW]|Sec\b|API\b|\d+\s*F\s*[NSEW]\s*L|\d+-\d+[NSEW]))',
            text, re.IGNORECASE,
        )
        if m:
            name = re.sub(r'\s+', ' ', m.group(1)).strip()
            if _is_garbled_well_name(name):
                name = _sanitize_garbled_well_name(name)
            if name and 3 < len(name) < 200 and not _is_rejected_well_name(name) and '__' not in name and 'wiltiams' not in name.lower():
                return name

    # same-line label preferred over next line (avoids truncation)
    m = re.search(r'Well\s+Name\s+&?\s*No\.?\s*[:\s]+([A-Za-z][A-Za-z0-9\s\-\.&]+?)(?:\n|$)', text, re.IGNORECASE)
    if m:
        name = re.sub(r'\s+', ' ', m.group(1)).strip()
        if 3 < len(name) < 200 and not _is_rejected_well_name(name):
            return name

    for m_iter in re.finditer(
        r'Well\s+Name\s+(?:and|an[·.]?d)\s+Number[^\n]*\n([^\n]+)', text, re.IGNORECASE,
    ):
        line = m_iter.group(1).strip()
        if not line or line.startswith('('):
            continue
        name = re.split(r'\s+(?:Before\b|After\b|(?:I\s+){2,}|Sec\s+\d|Spacing\b|T\d{3}N)', line, maxsplit=1)[0].strip()
        for pat in [
            r'\s+\d+\s+\d+\s+[NSEW]\s+\d+\s*[NSEW]?\s*$',
            r'\s+(?:[SN][EW][SN][EW]|LOT\d?|Sec\.?\s*\d+)\s+.*$',
            r'\s+\d{2,3}\s*[NnSs]\s+\d{2,3}\s*[WwEe].*$',
            r'\s+\d{2,3}\s+[wW]\s+.*$',
            r'\s+-+\.+.*$',
            r'\s+(?:McKenzie|Williams|Mountrail|Dunn|Stark)\s*$',
            r'\s+All\s+of\s+Sect.*$',
            r'\s+Sec\.\s+\d.*$',
            r'\s+~.*$',
        ]:
            name = re.sub(pat, '', name, flags=re.IGNORECASE).strip()
        name = re.sub(r'\s+', ' ', name)
        if _is_garbled_well_name(name):
            name = _sanitize_garbled_well_name(name)
        if name and 3 < len(name) < 200 and not _is_rejected_well_name(name):
            return name

    m = re.search(r'Well\s+Name\s*:\s*([A-Za-z][A-Za-z0-9\s\-\.&]+?)(?:\n|$)', text, re.IGNORECASE)
    if m:
        name = re.sub(r'\s+', ' ', m.group(1)).strip()
        if 3 < len(name) < 200 and not _is_rejected_well_name(name):
            return name

    for m in re.finditer(r'Well\s+Name\s*:\s*([A-Za-z][A-Za-z0-9\s\-\.&\']+?)(?:\s*API\s*#?|$|\n)', text, re.IGNORECASE):
        name = re.sub(r'\s+', ' ', m.group(1)).strip()
        if 3 < len(name) < 120 and not _is_rejected_well_name(name) and not _is_garbled_well_name(name):
            return name
    return None


def _normalize_address_spacing(addr):
    # fix P.O., comma spacing, Suite/Box etc
    if not addr or not isinstance(addr, str):
        return addr
    addr = addr.strip()
    addr = re.sub(r'^\s*co\s+', '', addr, flags=re.I)
    addr = re.sub(r'\s*,\s*', ', ', addr)
    addr = re.sub(r'P\s*\.\s*0\s*\.', 'P.O.', addr)
    addr = re.sub(r',\s*([A-Z])', r', \1', addr)
    addr = re.sub(r'(\d)([A-Z][a-z]+)', r'\1 \2', addr)
    addr = re.sub(r'([a-z])([A-Z][a-z]+)', r'\1 \2', addr)
    words = os.environ.get("ADDRESS_SPACING_WORDS", "Fannin,Suite,Street,Ave,Blvd,Drive,Box")
    for w in (x.strip() for x in words.split(",") if x.strip()):
        addr = re.sub(rf'([a-z0-9])({re.escape(w)})(?=\d|\s|$|,)', r'\1 \2', addr, flags=re.I)
    addr = re.sub(r'(Suite|Box)(\d)', r'\1 \2', addr, flags=re.I)
    addr = re.sub(r'(P\.O\.)([A-Z])', r'\1 \2', addr)
    return re.sub(r'\s+', ' ', addr).strip()


def extract_address(text):
    # truncate at surface owner so address does not include next field
    for sep in (r'Name\s+of\s+Surface\s+Owner', r'Surface\s+Owner\s+or\s+Tenant'):
        if re.search(sep, text, re.IGNORECASE):
            text = re.split(sep, text, maxsplit=1, flags=re.IGNORECASE)[0]
    for header in [
        r'Field\s+Address[^\n]*\n',
        r'Address\s+City\s+State\s+Zip\s*Code[^\n]*\n',
    ]:
        m = re.search(header + r'([A-Z0-9][A-Z0-9\s,#\-\.]+[A-Z]{2}\s+\d{5})', text, re.IGNORECASE)
        if m:
            raw = _apply_ocr_fixes(re.sub(r'\s+', ' ', m.group(1).strip()), "ADDRESS_OCR_FIXES",
                "Broadwa:Broadway,Broadwayy:Broadway,P .0.:P.O.,P. 0.:P.O.,Cit:City,Cityy:City, IN 9th: W 9th")
            return _normalize_address_spacing(raw)
    return None


def extract_county(text):
    # tokens that look like county but are form labels
    non_county = {'range', 'township', 'section', 'field', 'pool', 'state',
                  'code', 'baker', 'bakken', 'forks', 'address', 'city'}

    def ok(name):
        return (re.match(r'^[A-Za-z]+(?:\s+[A-Za-z]+)?$', name)
                and 2 < len(name) < 50
                and name.lower() not in non_county)

    m = re.search(r'([A-Z][a-zA-Z]+)\s+County,?\s+(?:North\s+Dakota|ND|N\.\s*Dakota)\b', text)
    if m and ok(m.group(1)):
        return m.group(1).strip()

    m = re.search(r'(?:Field|Pool)[^\n]*\bCounty\b[^\n]*\n([^\n]+)', text, re.IGNORECASE)
    if m:
        words = [w for w in m.group(1).strip().split() if re.match(r'^[A-Za-z]{3,}$', w)]
        for w in reversed(words):
            if ok(w):
                return w

    m = re.search(r'\bCounty\s*[,:]?\s*\n\s*([A-Za-z]{3,}(?:\s+[A-Za-z]{3,})?)\s*(?:\n|$)', text, re.IGNORECASE)
    if m and ok(m.group(1).strip()):
        return m.group(1).strip()

    m = re.search(r'County\s*[,:]\s*([A-Z][a-zA-Z]+)', text)
    if m and ok(m.group(1)):
        return m.group(1).strip()

    m = re.search(r'\b([A-Z][a-zA-Z]{2,})\s+County\b', text)
    if m and ok(m.group(1)):
        return m.group(1).strip()

    return None


def extract_field(text):
    # exclude pool/county/bad from field name
    bad = {'county', 'pool', 'field', 'address', 'city', 'state', 'name',
           'range', 'township', 'section', 'wildcat', 'development', 'extension'}

    pool_names = set()
    for pm in re.finditer(r'\bPool\s*\n([^\n]+)', text, re.IGNORECASE):
        for w in pm.group(1).strip().split():
            if re.match(r'^[A-Za-z]{3,}$', w):
                pool_names.add(w.lower())
    for pm in re.finditer(r'\bPool\s{2,}([A-Za-z]{3,})', text, re.IGNORECASE):
        pool_names.add(pm.group(1).strip().lower())
    pool_names -= bad

    county_names = set()
    for cm in re.finditer(r'([A-Za-z]{3,})\s+County', text, re.IGNORECASE):
        county_names.add(cm.group(1).strip().lower())
    for cm in re.finditer(r'\bCounty\s*[,:\n]\s*([A-Za-z]{3,})', text, re.IGNORECASE):
        county_names.add(cm.group(1).strip().lower())
    county_names -= bad

    def valid(word):
        return (re.match(r'^[A-Za-z]{3,}$', word)
                and word.lower() not in bad
                and word.lower() not in pool_names
                and word.lower() not in county_names)

    m = re.search(r'Field\s+(?:I\s+)?(?:Pool|Name)[^\n]*County[^\n]*\n\s*([^\n]+)', text, re.IGNORECASE)
    if m:
        words = [w for w in m.group(1).strip().split() if valid(w)]
        if words:
            return words[0]

    m = re.search(r'\bField\s*\n([^\n]+)\n\s*Pool\b', text, re.IGNORECASE)
    if m:
        words = [w for w in m.group(1).strip().split() if valid(w)]
        if words:
            return words[-1]

    phone = r'\(?\d{3}\)?[\-\s]?\d{3}[\-\s]\d{4}'
    for m_iter in re.finditer(r'\bField\s*\n([^\n]+)', text, re.IGNORECASE):
        line = m_iter.group(1).strip()
        parts = re.split(phone, line)
        if len(parts) > 1:
            after = parts[-1].strip()
            words = [w for w in after.split() if valid(w)]
            if words:
                return words[0]

    m = re.search(r'\bField\s*:\s*([A-Za-z]{3,})', text, re.IGNORECASE)
    if m and valid(m.group(1)):
        return m.group(1).strip()

    m = re.search(r'Field\s+Name\s*:\s*([A-Z][A-Za-z\s]{2,30})', text)
    if m:
        cand = m.group(1).strip().split()[0]
        if valid(cand):
            return cand

    return None


def clean_operator(cand):
    # strip trailing checkboxes and junk
    cand = re.sub(r'\s+', ' ', cand).strip()
    cand = re.sub(
        r'\s+(?:TIGHT|YES|NO\b|HOLE|CONFIDENTIAL|Company\s+man|Well[\-\s]*site|Geologist).*$',
        '', cand, flags=re.IGNORECASE,
    ).strip().rstrip(':')
    skip = ('address', 'company man', 'well-site', 'wellsite', 'geologist')
    if cand.lower().startswith(skip):
        return None
    if re.match(r'[A-Za-z]', cand) and 2 <= len(cand) <= 120:
        return cand
    return None


def extract_operator(text):
    # operator : or next line, stop at phone/well name
    m = re.search(
        r'Operator\s*:\s*([A-Za-z][A-Za-z0-9\s\-\.&,\'()]+?)(?:\s+Well\s+Name|\s+Enseco|\n)',
        text, re.IGNORECASE,
    )
    if m:
        c = clean_operator(m.group(1))
        if c:
            return c

    phone = r'\(?\d{3}\)?[\-\s]\d{3}[\-\s]\d{4}'
    for m_op in re.finditer(r'(?:^|\n)\s*Operator\b([^\n]*)', text, re.IGNORECASE):
        rest = m_op.group(1)
        if re.search(phone, rest):
            nm = re.match(r'[\s:]+([A-Za-z][A-Za-z0-9\s\-\.&,\']+?)\s+\(?\d{3}', rest)
            if nm:
                c = clean_operator(nm.group(1))
                if c:
                    return c
        else:
            nl = re.search(r'\n([^\n]+)', text[m_op.end():])
            if nl:
                raw = nl.group(1).strip()
                pm = re.search(phone, raw)
                if pm:
                    raw = raw[:pm.start()].strip()
                c = clean_operator(raw)
                if c:
                    return c

    m = re.search(r'Company\s*\n\s*([A-Za-z][A-Za-z0-9\s\-\.&,\']+?)(?:\s*\n)', text, re.IGNORECASE)
    if m:
        c = clean_operator(m.group(1))
        if c:
            return c

    return None


def extract_permit_number(text):
    # permit # or number from text
    m = re.search(r'Permit\s*(?:#|Number)?[:\s]*(\d[\d\-A-Za-z]*)', text, re.IGNORECASE)
    return m.group(1).strip() if m else None


def extract_permit_date(text):
    # permit date or date of permit
    for pat in [r'Permit\s*Date', r'Date\s+of\s+Permit']:
        m = re.search(pat + r'[:\s]*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})', text, re.IGNORECASE)
        if m:
            return m.group(1).strip()
    return None


def extract_total_depth(text):
    # total depth drilled or well depth, min from env
    min_depth = float(os.environ.get("MIN_TOTAL_DEPTH_FT", "0") or 0)
    for pat in [
        r'Total\s+Depth\s+Drilled\s*[:\s]\s*(\d[\d,]*\.?\d*)\s*[\'′]',
        r'Total\s+(?:Well\s+)?Depth[^\d]*(\d[\d,]*\.?\d*)\s*(ft|feet)?',
        r'Total\s*Depth[^\d]*(\d[\d,]*\.?\d*)\s*(ft|feet)?',
    ]:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            raw = m.group(1).replace(",", "")
            try:
                if float(raw) >= min_depth:
                    return raw + " ft"
            except ValueError:
                pass
    return None


def extract_formation(text):
    # formation name, max len from env, reject legal words
    m = re.search(r'\bFormation\s*[:\s]*([A-Za-z0-9\s\-\.]+?)(?=\n|$|\s{2,})', text, re.IGNORECASE)
    if m:
        cand = re.sub(r'\s+', ' ', m.group(1).strip())
        max_len = int(os.environ.get("FORMATION_MAX_LEN", "0") or 0)
        if cand and (max_len <= 0 or len(cand) <= max_len) and not re.search(r'\b(Director|contact|undersigned|required|please|would allow|information|the contract)\b', cand, re.I):
            return cand
    return None


def extract_stimulations(text):
    # split on date stimulated (OCR variants), parse blocks
    rows = []
    parts = re.split(
        r'(?:Date\s+S(?:[tl]i?mu\s*l?\s*a?\s*t?\s*e?\s*d|\s*t\s*i?\s*m\s*u\s*l\s*a\s*t\s*e\s*d)|'
        r'Stimulation\s+Date|Date\s+of\s+Stimulation)',
        text, flags=re.IGNORECASE,
    )
    blocks = [p.strip() for p in parts if p.strip()][1:]

    for block in blocks:
        lines = [l.strip() for l in block.split('\n') if l.strip()]
        if len(lines) < 2:
            continue
        form_ok = any(
            re.search(r'Stimulated\s+Form|Slimulaled\s+Form|Form(?:ation|alon|alion)|Formalion', l, re.IGNORECASE)
            for l in lines[:5]
        )
        if not form_ok:
            continue

        data_line = None
        for l in lines:
            if re.match(r'\d{1,2}[/\-]', l):
                data_line = l
                break
        if not data_line:
            for l in lines[1:8]:
                if len(re.findall(r'\d[\d,]*\.?\d*', l)) >= 4:
                    data_line = l
                    break
        if not data_line:
            for l in lines[1:8]:
                if re.search(r'Sand\s*Frac|Acid\s*Frac|Frac\b', l, re.I) and re.search(r'\d{6,}', l):
                    data_line = l
                    break
        if not data_line:
            continue

        date_stim = None
        m = re.match(r'(\d{1,2}[/\-\u2032\u2019\u0027\u2033\u2035\u00B4\u0060\u2018\u201C′]\d{1,2}[/\-\u2032\u2019\u0027\u2033\u2035\u00B4\u0060\u2018\u201C′]\d{2,4})', data_line)
        if m:
            raw_date = re.sub(r'[^\d/\-]', '/', m.group(1))
            date_stim = normalize_date_to_iso(raw_date) or raw_date

        formation = None
        m = re.search(
            r'(?:\d{1,2}[/\-\u2032\u2019′]\d{1,2}[/\-\u2032\u2019′]\d{2,4})\s+'
            r'([A-Za-z][A-Za-z\s]{2,25}?)\s+\d{3,}', data_line,
        )
        if m:
            formation = m.group(1).strip()
        if not formation:
            m = re.search(r'(?:\d{2,4})\s+([A-Za-z][A-Za-z\s]{2,20}?)\s+\d{3,}', data_line)
            if m:
                formation = m.group(1).strip()

        after = data_line
        if formation:
            idx = data_line.lower().find(formation.lower())
            if idx >= 0:
                after = data_line[idx + len(formation):]
        # ocr fix ll/1 in numbers
        after = re.sub(r'(?<!\w)ll(\d{2,})', r'11\1', after)
        after = re.sub(r'(?<!\w)[lI](\d{3,})', r'1\1', after)
        nums = [parse_num(n) for n in re.findall(r'[\d,]+\.?\d*', after)]
        nums = [v for v in nums if v is not None]

        # skip year/small so not top_ft
        i = 0
        while i < len(nums) and (nums[i] < 100 or 1990 <= nums[i] <= 2100):
            i += 1
        top_ft      = nums[i] if i < len(nums) and nums[i] > 100 else None
        bottom_ft   = nums[i + 1] if i + 1 < len(nums) and nums[i + 1] > 100 else None
        stim_stages = int(nums[i + 2]) if i + 2 < len(nums) and nums[i + 2] < 200 else None
        volume      = nums[i + 3] if i + 3 < len(nums) else None

        volume_units = None
        m = re.search(r'\b(Barrels|BBL[Ss]?|Gallons?|GAL[Ss]?)\b', block, re.IGNORECASE)
        if m:
            volume_units = m.group(1)

        type_treatment = None
        lbs_proppant = None
        max_pressure = None
        max_rate = None
        acid_pct = None

        treat_lines = []
        in_treat = False
        for l in lines:
            if re.search(r'Type\s+Treat\s*ment', l, re.IGNORECASE):
                in_treat = True
                continue
            if in_treat:
                if re.match(r'Details?', l, re.IGNORECASE) or re.match(r'Date\s+S', l, re.IGNORECASE):
                    break
                if re.search(r'Mesh|White|Ceramic|Resin|CRC', l, re.IGNORECASE):
                    break
                treat_lines.append(l)

        for l in treat_lines:
            if not type_treatment:
                m = re.search(r'(Sand\s*Frac|Acid\s*Frac|Frac|Acid)\b', l, re.IGNORECASE)
                if m:
                    type_treatment = m.group(1).strip()
            if acid_pct is None:
                am = re.search(r'(?:Acid|HCl)\s*[:%]?\s*(\d{1,3}(?:\.\d+)?)\s*%', l, re.IGNORECASE)
                if not am:
                    am = re.search(r'(\d{1,3}(?:\.\d+)?)\s*%\s*(?:Acid|HCl)', l, re.IGNORECASE)
                if not am and re.match(r'\s*Acid\b', l, re.IGNORECASE):
                    am = re.search(r'\bAcid\s+(\d{1,3}(?:\.\d+)?)\b', l, re.IGNORECASE)
                if am:
                    try:
                        if 0 < float(am.group(1)) <= 100:
                            acid_pct = am.group(1)
                    except ValueError:
                        pass

        treat_blob = ' '.join(treat_lines)
        treat_vals = [parse_num(n) for n in re.findall(r'[\d,]+\.?\d*', treat_blob)]
        treat_vals = [v for v in treat_vals if v is not None]

        if acid_pct is not None:
            try:
                acid_val = float(acid_pct)
                if acid_val in treat_vals:
                    treat_vals.remove(acid_val)
            except ValueError:
                pass

        if treat_vals:
            for v in treat_vals:
                if v > 100000:
                    lbs_proppant = v
                    break
            rest = [v for v in treat_vals if v != lbs_proppant]
            for v in rest:
                if 1000 < v < 20000 and max_pressure is None:
                    max_pressure = v
                elif 0 < v < 200 and max_rate is None:
                    max_rate = v

        if acid_pct is None:
            am = re.search(r'(?:Acid|HCl)\s*[:%]?\s*(\d{1,3}(?:\.\d+)?)\s*%', block, re.IGNORECASE)
            if not am:
                am = re.search(r'(\d{1,3}(?:\.\d+)?)\s*%\s*(?:Acid|HCl)', block, re.IGNORECASE)
            if not am:
                am = re.search(r'\bAcid\s+(\d{1,3}(?:\.\d+)?)\b', block, re.IGNORECASE)
            if am:
                try:
                    if 0 < float(am.group(1)) <= 100:
                        acid_pct = am.group(1)
                except ValueError:
                    pass

        detail_lines = []
        in_details = False
        for l in lines:
            if re.match(r'Details?', l, re.IGNORECASE):
                in_details = True
                rest = re.sub(r'^Details?\s*', '', l, flags=re.IGNORECASE).strip()
                if rest and re.search(r'\d', rest):
                    detail_lines.append(rest)
                continue
            if in_details:
                if re.match(r'Date\s+S|ADDITIONAL|I hereby|Type\s+Treatment|^\s*$', l, re.IGNORECASE):
                    break
                if re.search(r'Stimulated\s+Formation|Volume\s+Units|Maximum\s+Treatment|'
                             r'Lbs\s+Proppant|Stimulation\s+Stages|Top\s*\(Ft\)|Bottom\s*\(Ft\)',
                             l, re.IGNORECASE):
                    continue
                if re.search(r'\d', l):
                    detail_lines.append(l)
        details = '; '.join(detail_lines) if detail_lines else None

        if not any([lbs_proppant, volume, formation]):
            continue

        date_val = (date_stim or '').strip() or None
        formation_val = (formation or '').strip() or None

        rows.append({
            'date_stimulated': date_val,
            'stimulated_formation': formation_val,
            'top_ft': top_ft,
            'bottom_ft': bottom_ft,
            'stimulation_stages': stim_stages,
            'volume': volume,
            'volume_units': volume_units,
            'type_treatment': type_treatment,
            'acid_pct': acid_pct,
            'lbs_proppant': lbs_proppant,
            'max_treatment_pressure_psi': max_pressure,
            'max_treatment_rate': max_rate,
            'details': details,
        })

    # fallback when data line appears before header (table layout)
    existing_lbs = {r['lbs_proppant'] for r in rows if r.get('lbs_proppant')}
    for m in re.finditer(
        r'(Sand\s*Frac|Acid\s*Frac|Frac|Acid)\s+([\d,\s]+)',
        text, re.IGNORECASE,
    ):
        type_treatment = m.group(1).strip()
        nums = [parse_num(x) for x in re.findall(r'[\d,]+\.?\d*', m.group(2))]
        nums = [v for v in nums if v is not None and v > 100000]
        if nums and nums[0] not in existing_lbs:
            existing_lbs.add(nums[0])
            rows.append({
                'date_stimulated': None, 'stimulated_formation': None, 'top_ft': None, 'bottom_ft': None,
                'stimulation_stages': None, 'volume': None, 'volume_units': None,
                'type_treatment': type_treatment, 'acid_pct': None, 'lbs_proppant': nums[0],
                'max_treatment_pressure_psi': nums[1] if len(nums) > 1 and 1000 < nums[1] < 20000 else None,
                'max_treatment_rate': nums[2] if len(nums) > 2 and 0 < nums[2] < 200 else None,
                'details': None,
            })

    return rows


# table header label -> well dict key
TABLE_LABELS = {
    "api": "api_number", "api number": "api_number", "api #": "api_number",
    "well name": "well_name",
    "latitude": "latitude_raw", "lat": "latitude_raw",
    "longitude": "longitude_raw", "long": "longitude_raw",
    "address": "address", "field address": "address",
    "county": "county", "field": "field", "field/pool": "field",
    "operator": "operator",
    "permit number": "permit_number", "permit #": "permit_number",
    "permit date": "permit_date",
    "total depth": "total_depth", "depth": "total_depth",
    "formation": "formation",
}


def extract_from_tables(tables):
    # two-col as key/val, else dump rows as text
    text_lines = []
    kv = {}

    for table in (tables or []):
        if not table or len(table) < 2:
            continue
        if len(table[0]) == 2:
            for row in table:
                if len(row) >= 2 and row[0] and row[1]:
                    label = re.sub(r'\s+', ' ', str(row[0])).strip().lower()
                    val = str(row[1] or '').strip()
                    if not val:
                        continue
                    text_lines.append(f"{row[0]} {val}")
                    for tbl_label, well_key in TABLE_LABELS.items():
                        if tbl_label in label or label in tbl_label:
                            kv.setdefault(well_key, val)
                            break
        else:
            for row in table:
                text_lines.append(' '.join(str(c or '').strip() for c in row if c))

    return text_lines, kv



def extract_from_pdf(pdf_path, max_pages=None):
    # extract all fields, backfill from tables when missed
    well_file = extract_well_file_from_filename(os.path.basename(pdf_path))
    result = {
        'api_number': None, 'well_file_no': well_file, 'well_name': None,
        'latitude': None, 'longitude': None, 'address': None,
        'county': None, 'field': None, 'operator': None,
        'permit_number': None, 'permit_date': None, 'total_depth': None,
        'formation': None, 'stimulation_notes': None,
        'stimulation_rows': [], 'raw_extract': None,
        'pdf_source': os.path.basename(pdf_path),
    }

    full_text = ''
    all_table_lines = []
    all_kv = {}

    try:
        with pdfplumber.open(pdf_path) as pdf:
            pages = pdf.pages if max_pages is None else pdf.pages[:max_pages]
            for page in pages:
                t = page.extract_text()
                if t:
                    full_text += '\n' + t
                tbl_lines, kv = extract_from_tables(page.extract_tables())
                all_table_lines.extend(tbl_lines)
                for k, v in kv.items():
                    all_kv.setdefault(k, v)
    except Exception as e:
        print(f"  Error reading {pdf_path}: {e}")
        return result

    if all_table_lines:
        full_text += '\n' + '\n'.join(all_table_lines)

    # optional OCR when text short (env controls when)
    if os.environ.get("USE_OCR_FALLBACK") and pytesseract and convert_from_path:
        min_chars_s = os.environ.get("OCR_FALLBACK_MIN_CHARS", "").strip()
        run_ocr = not min_chars_s or len(full_text.strip()) < int(min_chars_s)
        if run_ocr:
            try:
                max_pages_s = os.environ.get("OCR_FALLBACK_MAX_PAGES", "").strip()
                last_page = int(max_pages_s) if max_pages_s else None
                images = convert_from_path(pdf_path, first_page=1, last_page=last_page, dpi=200)
                full_text = "\n".join(pytesseract.image_to_string(img) for img in images) + "\n" + full_text
            except Exception:
                pass

    if not full_text.strip():
        return result

    result['raw_extract'] = full_text
    result['api_number'] = extract_api(full_text)
    if not result['well_file_no']:
        result['well_file_no'] = extract_well_file_from_text(full_text)
    result['well_name'] = _apply_ocr_fixes(extract_well_name(full_text, well_file), "WELL_NAME_OCR_FIXES",
        "Federa1:Federal,Cc lumbus:Columbus,Chalmes:Chalmers,lnnoko:Innoko,Gramma:Gamma")
    result['latitude'] = extract_latitude(full_text)
    result['longitude'] = extract_longitude(full_text)
    result['address'] = extract_address(full_text)
    result['county'] = extract_county(full_text)
    result['field'] = extract_field(full_text)
    result['operator'] = extract_operator(full_text)
    result['permit_number'] = extract_permit_number(full_text)
    result['permit_date'] = extract_permit_date(full_text)
    result['total_depth'] = extract_total_depth(full_text)
    result['formation'] = extract_formation(full_text)
    result['stimulation_rows'] = extract_stimulations(full_text)

    if result['stimulation_rows']:
        parts = []
        for sr in result['stimulation_rows']:
            bits = []
            if sr.get('stimulated_formation'):
                bits.append(sr['stimulated_formation'])
            if sr.get('lbs_proppant'):
                bits.append(f"{sr['lbs_proppant']:.0f} lbs proppant")
            if sr.get('type_treatment'):
                bits.append(sr['type_treatment'])
            if bits:
                parts.append(', '.join(bits))
        result['stimulation_notes'] = '; '.join(parts) if parts else None

    # backfill from table kv when extract missed
    for key in ('api_number', 'well_name', 'address', 'county', 'field', 'operator',
                'permit_number', 'permit_date', 'total_depth', 'formation'):
        if all_kv.get(key) and not result[key]:
            val = str(all_kv[key]).strip()
            if key == 'well_name':
                val = _apply_ocr_fixes(val, "WELL_NAME_OCR_FIXES",
                    "Federa1:Federal,Cc lumbus:Columbus,Chalmes:Chalmers,lnnoko:Innoko,Gramma:Gamma")
            elif key == 'address':
                val = _apply_ocr_fixes(val, "ADDRESS_OCR_FIXES",
                    "Broadwa:Broadway,Broadwayy:Broadway,P .0.:P.O.,P. 0.:P.O.,Cit:City,Cityy:City, IN 9th: W 9th")
                val = _normalize_address_spacing(val)
            result[key] = val

    # fallback lat/lon from table
    if all_kv.get('latitude_raw') and result['latitude'] is None:
        try:
            raw = str(all_kv['latitude_raw']).strip()
            if re.search(r'[°\u00B0]', raw):
                lat = dms_to_decimal(raw + (' N' if 'N' not in raw.upper() and 'S' not in raw.upper() else ''))
            else:
                lat = float(raw)
            if lat is not None and -90 <= lat <= 90:
                result['latitude'] = round(lat, 6)
        except (ValueError, TypeError):
            pass

    if all_kv.get('longitude_raw') and result['longitude'] is None:
        try:
            raw = str(all_kv['longitude_raw']).strip()
            if re.search(r'[°\u00B0]', raw):
                lon = dms_to_decimal(raw + (' W' if 'W' not in raw.upper() and 'E' not in raw.upper() else ''))
            else:
                lon = float(raw)
                if lon is not None and lon > 0:
                    lon = -lon
            if lon is not None and -180 <= lon <= 180:
                result['longitude'] = round(lon, 6)
        except (ValueError, TypeError):
            pass

    return result


def setup_db(conn):
    # create wells and stimulation_data tables
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS wells (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        api_number TEXT,
        well_file_no TEXT,
        well_name TEXT,
        latitude REAL,
        longitude REAL,
        address TEXT,
        county TEXT,
        field TEXT,
        operator TEXT,
        permit_number TEXT,
        permit_date TEXT,
        total_depth TEXT,
        formation TEXT,
        stimulation_notes TEXT,
        raw_extract TEXT,
        pdf_source TEXT UNIQUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS stimulation_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        well_id INTEGER NOT NULL,
        date_stimulated TEXT,
        stimulated_formation TEXT,
        top_ft REAL,
        bottom_ft REAL,
        stimulation_stages INTEGER,
        volume REAL,
        volume_units TEXT,
        type_treatment TEXT,
        acid_pct TEXT,
        lbs_proppant REAL,
        max_treatment_pressure_psi REAL,
        max_treatment_rate REAL,
        details TEXT,
        FOREIGN KEY (well_id) REFERENCES wells(id)
    );
    CREATE INDEX IF NOT EXISTS idx_wells_api ON wells(api_number);
    CREATE INDEX IF NOT EXISTS idx_wells_file ON wells(well_file_no);
    CREATE INDEX IF NOT EXISTS idx_stim_well ON stimulation_data(well_id);
    """)


WELL_COLS = (
    'api_number', 'well_file_no', 'well_name', 'latitude', 'longitude',
    'address', 'county', 'field', 'operator', 'permit_number', 'permit_date',
    'total_depth', 'formation', 'stimulation_notes', 'raw_extract', 'pdf_source',
)

STIM_COLS = (
    'well_id', 'date_stimulated', 'stimulated_formation', 'top_ft', 'bottom_ft',
    'stimulation_stages', 'volume', 'volume_units', 'type_treatment', 'acid_pct',
    'lbs_proppant', 'max_treatment_pressure_psi', 'max_treatment_rate', 'details',
)


def main():
    # discover PDFs, extract each, upsert wells and stim rows
    parser = argparse.ArgumentParser()
    parser.add_argument('--pdf-dir', default=PDF_DIR)
    parser.add_argument('--db-path', default=DB_PATH)
    parser.add_argument('--max-pages', type=int, default=None, help="Max pages per PDF (default: no limit, parse entire PDF)")
    parser.add_argument('--limit', type=int, default=None, help="Only process first N PDFs")
    parser.add_argument('--files', type=str, default=None, help="Comma-separated PDF filenames (e.g. W28651.pdf,W20197.pdf)")
    args = parser.parse_args()

    base = Path(__file__).parent
    pdf_dir = (base / args.pdf_dir).resolve()
    if not pdf_dir.exists():
        print(f"Error: directory not found: {pdf_dir}")
        return 1

    pdf_files = sorted(pdf_dir.glob('*.pdf'))
    if args.files:
        names = {n.strip() for n in args.files.split(',') if n.strip()}
        pdf_files = [p for p in pdf_files if p.name in names]
    if args.limit:
        pdf_files = pdf_files[:args.limit]
    if not pdf_files:
        print("No PDFs found.")
        return 1

    db_path = (base / args.db_path).resolve()
    conn = sqlite3.connect(str(db_path))
    setup_db(conn)
    cur = conn.cursor()

    # optional text fields -> N/A when missing
    na_fields = [
        'well_name', 'address', 'county', 'field', 'operator',
        'permit_number', 'permit_date', 'total_depth', 'formation', 'stimulation_notes',
    ]

    inserted = 0
    for pdf_path in pdf_files:
        print(f"Processing {pdf_path.name}...")
        data = extract_from_pdf(str(pdf_path), max_pages=args.max_pages)

        if not data['well_file_no']:
            data['well_file_no'] = extract_well_file_from_filename(pdf_path.name)

        if data.get('field'):
            data['field'] = data['field'].split('\n')[0].strip()
            raw = data.get('raw_extract') or ''
            pool_words = set()
            for pm in re.finditer(r'\bPool\s*\n([^\n]+)', raw, re.IGNORECASE):
                for w in pm.group(1).strip().split():
                    if re.match(r'^[A-Za-z]{3,}$', w):
                        pool_words.add(w.lower())
            for pm in re.finditer(r'\bPool\s{2,}([A-Za-z]{3,})', raw, re.IGNORECASE):
                pool_words.add(pm.group(1).strip().lower())
            # treat pool as not field
            if data['field'].lower().split()[0] in pool_words:
                data['field'] = None

        for key in na_fields:
            data[key] = clean_value(data.get(key))

        if data['field'] not in (None, 'N/A'):
            data['field'] = data['field'].title()

        data['api_number'] = clean_value(data['api_number'])
        if data['api_number'] == 'N/A':
            data['api_number'] = None

        raw_extract = data.get('raw_extract') or ''

        try:
            placeholders = ','.join(['?'] * len(WELL_COLS))
            col_str = ','.join(WELL_COLS)
            update_str = ','.join(f'{c}=excluded.{c}' for c in WELL_COLS if c != 'pdf_source')

            values = tuple(
                data.get(c) if c != 'raw_extract' else raw_extract
                for c in WELL_COLS
            )
            cur.execute(
                f"INSERT INTO wells ({col_str}) VALUES ({placeholders}) "
                f"ON CONFLICT(pdf_source) DO UPDATE SET {update_str}",
                values,
            )
            # same pdf: overwrite well and stim rows
            cur.execute("SELECT id FROM wells WHERE pdf_source=?", (data['pdf_source'],))
            well_id = cur.fetchone()[0]
            cur.execute("DELETE FROM stimulation_data WHERE well_id=?", (well_id,))
            for r in data.get('stimulation_rows') or []:
                stim_vals = tuple(
                    well_id if c == 'well_id' else r.get(c)
                    for c in STIM_COLS
                )
                cur.execute(
                    f"INSERT INTO stimulation_data ({','.join(STIM_COLS)}) "
                    f"VALUES ({','.join(['?'] * len(STIM_COLS))})",
                    stim_vals,
                )

            inserted += 1
        except Exception as e:
            print(f"  Warning: {e}")

    conn.commit()
    print(f"\nDone. Inserted {inserted} wells.")
    cur.execute("SELECT COUNT(*) FROM wells")
    print("Total wells:", cur.fetchone()[0])
    cur.execute("SELECT COUNT(*) FROM wells WHERE latitude IS NOT NULL AND longitude IS NOT NULL")
    print("With coords:", cur.fetchone()[0])
    cur.execute("SELECT COUNT(*) FROM stimulation_data")
    print("Stim rows:", cur.fetchone()[0])

    conn.close()
    return 0


if __name__ == '__main__':
    exit(main())
