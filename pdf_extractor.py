#!/usr/bin/env python3
# Extract well info and stimulation data from PDFs into SQLite.

import argparse
import os
import re
import sqlite3
from pathlib import Path

try:
    import pdfplumber
except ImportError:
    print("pdfplumber not installed. Run: pip install pdfplumber")
    exit(1)

PDF_DIR = "pdfs"
DB_PATH = "oil_wells.db"


def normalize_dms(text):
    # unify degree/min/sec symbols so regex can match
    text = re.sub(r'[\u00BA\u02DA\u00B7\u02D9]', '\u00B0', text)
    text = re.sub(r'[\u2032\u2019\u02BC\u02B9`\u00B4]', "'", text)
    text = re.sub(r'[\u2033\u201D\u02BA]', '"', text)
    text = text.replace('~', '')
    return text


def dms_to_decimal(dms_str):
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
    if val is None or (isinstance(val, str) and not val.strip()):
        return 'N/A'
    if isinstance(val, str):
        val = re.sub(r'<[^>]+>', '', val)
        val = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]', '', val)
        val = val.strip()
    return val


def extract_api(text):
    # prefer survey/permit block so we don't pick up API from commingling
    survey = re.search(
        r'(?:Directional\s+Survey|Survey\s+(?:Report|Certification)|'
        r'Well\s+Completion|APPLICATION\s+FOR\s+PERMIT)[^\n]*((?:.*\n){0,20})',
        text, re.IGNORECASE,
    )
    regions = [survey.group(0)] if survey else []
    regions.append(text)

    patterns = [
        r'API\s*[:#]?\s*(\d{2})\s*[-]\s*(\d{3})\s*[-]\s*(\d{5})',
        r'API\s*[:#]?\s*(\d{2})\s*[-]?\s*(\d{3})\s*[-]?\s*(\d{5})',
        r'API\s*[:#]?\s*(\d{10,11})',
    ]
    for region in regions:
        for p in patterns:
            m = re.search(p, region, re.IGNORECASE)
            if m:
                if len(m.groups()) == 3:
                    return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
                raw = m.group(1)
                if len(raw) >= 10:
                    return f"{raw[:2]}-{raw[2:5]}-{raw[5:10]}"
                return raw
    return None


def extract_well_file_from_filename(filename):
    m = re.match(r'W(\d+)\.pdf', filename, re.IGNORECASE)
    return m.group(1) if m else None


def extract_well_file_from_text(text):
    m = re.search(r'Well\s*File\s*(?:#|Number)?[:\s]*(\d{4,6})', text, re.IGNORECASE)
    if m:
        return m.group(1)
    m = re.search(r'File\s*#\s*(\d{4,6})', text, re.IGNORECASE)
    return m.group(1) if m else None


def extract_latitude(text):
    norm = normalize_dms(text)

    dms_pats = [
        r'Well\s+Coordinates[^(]*\(\s*(\d+)\s*[°]\s*(\d+)\s*[\']?\s*([\d.]+)\s*["]?\s*N\s*[,\)]',
        r'Latitude\s+of\s+Well\s+Head[^\d]*(\d+)\s*[°]\s*(\d+)\s*[\']?\s*([\d.]+)\s*["]?',
        r'Lat(?:itude|ittude)?\s*[:\s]\s*(\d{2})\s*[°]\s*(\d{1,2})\s*[\']?\s*([\d.]+)\s*["]?\s*N',
        r'(\d{2})\s*[°]\s*(\d{1,2})\s*[\']\s*([\d.]+)\s*["]?\s*N\b',
    ]
    for p in dms_pats:
        m = re.search(p, norm, re.IGNORECASE)
        if m:
            dms = f"{m.group(1)}° {m.group(2)}' {m.group(3)}\" N"
            dec = dms_to_decimal(dms)
            if dec is not None and -90 <= dec <= 90:
                return round(dec, 6)

    m = re.search(
        r'(?:Survey\s+)?Lat(?:itude|ittude)?\s*[:\s]\s*(\d{2}\.\d{2,6})\s*(?:deg\.?\s*[NS]?)?',
        norm, re.IGNORECASE,
    )
    if m:
        try:
            v = float(m.group(1))
            if -90 <= v <= 90:
                return round(v, 6)
        except ValueError:
            pass

    m = re.search(r'\bLat(?:itude|ittude)?\b[^\d\n]{0,20}(\d{2}\.\d{2,6})', norm, re.IGNORECASE)
    if m:
        try:
            v = float(m.group(1))
            if -90 <= v <= 90:
                return round(v, 6)
        except ValueError:
            pass

    return None


def extract_longitude(text):
    norm = normalize_dms(text)

    dms_pats = [
        r'Well\s+Coordinates[^)]*N\s*[,\)]\s*(\d+)\s*[°]\s*(\d+)\s*[\']?\s*([\d.]+)\s*["]?\s*W',
        r'Longitude\s+of\s+Well\s+Head[^\d]*(-?\d+)\s*[°]\s*(\d+)\s*[\']?\s*([\d.]+)\s*["]?\s*W?',
        r'Long(?:itude)?\s*[:\s]\s*(-?\d{2,3})\s*[°]\s*(\d{1,2})\s*[\']?\s*([\d.]+)\s*["]?\s*W',
        r'Long(?:itude)?\s*[:\s]\s*(-?\d{2,3})\s*["\u201C]\s*(\d{1,2})\s*[\']?\s*([\d.]+)\s*["]?\s*W',
        r'(\d{2,3})\s*[°]\s*(\d{1,2})\s*[\']\s*([\d.]+)\s*["]?\s*W\b',
    ]
    for p in dms_pats:
        m = re.search(p, norm, re.IGNORECASE)
        if m:
            deg = int(m.group(1).lstrip('-'))
            if deg < 90:  # probably lat (ND is 100+ W)
                continue
            dms = f"-{deg}° {m.group(2)}' {m.group(3)}\" W"
            dec = dms_to_decimal(dms)
            if dec is not None and dec > 0:
                dec = -dec
            if dec is not None and -180 <= dec <= 180:
                return round(dec, 6)

    m = re.search(
        r'(?:Survey\s+)?\bLong(?:itude)?\b\s*[:\s]\s*(-?\d{2,3}\.\d{2,6})\s*(?:deg\.?\s*[WE]?)?',
        norm, re.IGNORECASE,
    )
    if m:
        try:
            v = float(m.group(1))
            if v > 0:
                v = -v
            if -180 <= v <= 180:
                return round(v, 6)
        except ValueError:
            pass

    m = re.search(r'\bLong(?:itude)?\b[^\d\n]{0,20}(-?\d{2,3}\.\d{2,6})', norm, re.IGNORECASE)
    if m:
        try:
            v = float(m.group(1))
            if v > 0:
                v = -v
            if -180 <= v <= 180:
                return round(v, 6)
        except ValueError:
            pass

    return None


def extract_well_name(text, well_file_no=None):
    if well_file_no:
        m = re.search(
            rf'(?:Well\s+)?File\s*#?\s*:?\s*{re.escape(well_file_no)}\s+'
            r'([A-Za-z][A-Za-z0-9\s\-\.&\']+?)'
            r'(?:\s+(?:LOT\d?|[SN][EW][SN][EW]|Sec\b|API\b|\d+\s*F\s*[NSEW]\s*L|\d+-\d+[NSEW]))',
            text, re.IGNORECASE,
        )
        if m:
            name = re.sub(r'\s+', ' ', m.group(1)).strip()
            if 3 < len(name) < 80:
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
        if 3 < len(name) < 80:
            return name

    m = re.search(r'Well\s+Name\s*:\s*([A-Za-z][A-Za-z0-9\s\-\.&]{2,60}?)(?:\n|$)', text, re.IGNORECASE)
    if m:
        name = re.sub(r'\s+', ' ', m.group(1)).strip()
        if 3 < len(name) < 80:
            return name

    return None


def extract_address(text):
    for header in [
        r'Field\s+Address[^\n]*\n',
        r'Address\s+City\s+State\s+Zip\s*Code[^\n]*\n',
    ]:
        m = re.search(header + r'([A-Z0-9][A-Z0-9\s,#\-\.]+[A-Z]{2}\s+\d{5})', text, re.IGNORECASE)
        if m:
            return re.sub(r'\s+', ' ', m.group(1).strip())[:200]
    return None


def extract_county(text):
    # form labels that appear next to County but aren't county names
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
    # avoid returning pool/formation/county as field
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
    cand = re.sub(r'\s+', ' ', cand).strip()
    # strip checkboxes and trailing junk that got merged in
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
    m = re.search(r'Permit\s*(?:#|Number)?[:\s]*(\d[\d\-A-Za-z]*)', text, re.IGNORECASE)
    return m.group(1).strip()[:64] if m else None


def extract_permit_date(text):
    for pat in [r'Permit\s*Date', r'Date\s+of\s+Permit']:
        m = re.search(pat + r'[:\s]*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})', text, re.IGNORECASE)
        if m:
            return m.group(1).strip()[:32]
    return None


def extract_total_depth(text):
    m = re.search(r'Total\s*Depth[^\d]*(\d[\d,]*\.?\d*)\s*(ft|feet)?', text, re.IGNORECASE)
    if m:
        return m.group(1).replace(",", "") + " ft"
    m = re.search(r'Depth\s*[:\s]*(\d[\d,]*\.?\d*)\s*(ft|feet)\b', text, re.IGNORECASE)
    if m:
        return m.group(1).replace(",", "") + " ft"
    return None


def extract_formation(text):
    m = re.search(r'Formation\s*[:\s]*([A-Za-z0-9\s\-\.]+?)(?=\n|$|\s{2})', text, re.IGNORECASE)
    if m:
        cand = m.group(1).strip()
        if 1 <= len(cand) <= 80:
            return re.sub(r'\s+', ' ', cand)
    return None


def extract_stimulations(text):
    # split on Date Stimulated header, one block per record
    rows = []
    blocks = re.split(r'Date\s+S[tl]i?mu\s*l?\s*a?\s*t?\s*e?\s*d', text, flags=re.IGNORECASE)

    for block in blocks[1:]:
        block = block[:2500]
        lines = [l.strip() for l in block.split('\n') if l.strip()]
        if len(lines) < 2:
            continue

        if not any(re.search(r'Stimulated\s+Form|Form(?:ation|alon|alion)', l, re.IGNORECASE) for l in lines[:2]):
            continue

        data_line = None
        for l in lines:
            if re.match(r'\d{1,2}[/\-]', l):
                data_line = l
                break
        if not data_line:
            for l in lines[1:4]:
                if len(re.findall(r'\d[\d,]*\.?\d*', l)) >= 4:
                    data_line = l
                    break
        if not data_line:
            continue

        date_stim = None
        m = re.match(r'(\d{1,2}[/\-\u2032\u2019\u0027\u2033\u2035\u00B4\u0060\u2018\u201C′]\d{1,2}[/\-\u2032\u2019\u0027\u2033\u2035\u00B4\u0060\u2018\u201C′]\d{2,4})', data_line)
        if m:
            date_stim = re.sub(r'[^\d/\-]', '/', m.group(1))

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
        # OCR fix: ll->11, l->1 in numbers
        after = re.sub(r'(?<!\w)ll(\d{2,})', r'11\1', after)
        after = re.sub(r'(?<!\w)[lI](\d{3,})', r'1\1', after)
        nums = [parse_num(n) for n in re.findall(r'[\d,]+\.?\d*', after)]
        nums = [v for v in nums if v is not None]

        top_ft      = nums[0] if len(nums) > 0 and nums[0] > 100 else None
        bottom_ft   = nums[1] if len(nums) > 1 and nums[1] > 100 else None
        stim_stages = int(nums[2]) if len(nums) > 2 and nums[2] < 200 else None
        volume      = nums[3] if len(nums) > 3 else None

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
        details = '; '.join(detail_lines)[:500] if detail_lines else None

        if not any([lbs_proppant, volume, formation]):  # skip empty blocks
            continue

        rows.append({
            'date_stimulated': date_stim,
            'stimulated_formation': formation,
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

    return rows


# table header text -> our well dict keys
TABLE_LABELS = {
    "api": "api_number", "api number": "api_number", "api #": "api_number",
    "well name": "well_name",
    "latitude": "latitude_raw", "longitude": "longitude_raw",
    "address": "address", "field address": "address",
    "county": "county", "field": "field", "field/pool": "field",
    "operator": "operator",
    "permit number": "permit_number", "permit #": "permit_number",
    "permit date": "permit_date",
    "total depth": "total_depth", "depth": "total_depth",
    "formation": "formation",
}


def extract_from_tables(tables):
    # 2-col tables = key/val; others we just dump as text lines
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



def extract_from_pdf(pdf_path, max_pages=300):
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
            for i, page in enumerate(pdf.pages[:max_pages]):
                t = page.extract_text()
                if t:
                    full_text += '\n' + t
                if i < 15:  # tables only from first 15 pages, rest is commingling
                    tbl_lines, kv = extract_from_tables(page.extract_tables())
                    all_table_lines.extend(tbl_lines)
                    for k, v in kv.items():
                        all_kv.setdefault(k, v)
    except Exception as e:
        print(f"  Error reading {pdf_path}: {e}")
        return result

    if all_table_lines:
        full_text += '\n' + '\n'.join(all_table_lines)

    if not full_text.strip():
        return result

    result['raw_extract'] = full_text
    result['api_number'] = extract_api(full_text)
    if not result['well_file_no']:
        result['well_file_no'] = extract_well_file_from_text(full_text)
    result['well_name'] = extract_well_name(full_text, well_file)
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
        result['stimulation_notes'] = '; '.join(parts)[:500] if parts else None

    # backfill from tables when text extraction missed it
    simple_overrides = {
        'api_number': 32, 'well_name': 200, 'address': 200, 'county': 64,
        'field': 128, 'operator': 200, 'permit_number': 64, 'permit_date': 32,
        'total_depth': 32, 'formation': 80,
    }
    for key, maxlen in simple_overrides.items():
        if all_kv.get(key) and not result[key]:
            result[key] = str(all_kv[key]).strip()[:maxlen]

    # fallback lat/lon from tables
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
    parser = argparse.ArgumentParser()
    parser.add_argument('--pdf-dir', default=PDF_DIR)
    parser.add_argument('--db-path', default=DB_PATH)
    parser.add_argument('--max-pages', type=int, default=300)
    parser.add_argument('--limit', type=int, default=None, help="Only process first N PDFs")
    args = parser.parse_args()

    base = Path(__file__).parent
    pdf_dir = (base / args.pdf_dir).resolve()
    if not pdf_dir.exists():
        print(f"Error: directory not found: {pdf_dir}")
        return 1

    pdf_files = sorted(pdf_dir.glob('*.pdf'))
    if args.limit:
        pdf_files = pdf_files[:args.limit]
    if not pdf_files:
        print("No PDFs found.")
        return 1

    db_path = (base / args.db_path).resolve()
    conn = sqlite3.connect(str(db_path))
    setup_db(conn)
    cur = conn.cursor()

    # these get N/A when missing
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
            if data['field'].lower().split()[0] in pool_words:  # that's pool not field
                data['field'] = None

        for key in na_fields:
            data[key] = clean_value(data.get(key))

        if data['field'] not in (None, 'N/A'):
            data['field'] = data['field'].title()

        data['api_number'] = clean_value(data['api_number'])
        if data['api_number'] == 'N/A':
            data['api_number'] = None  # None not N/A so it can stay as join key

        raw_extract = (data.get('raw_extract') or '')[:500000]

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
            # same PDF again = overwrite that well and its stim rows
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
