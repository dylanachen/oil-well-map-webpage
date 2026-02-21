# oil-well-map-webpage

Apache webpage serving an interactive map of scraped oil well data. This repo includes the data pipeline: PDF extraction and SQLite database of well information and stimulation data.

## What it does

- **PDF extraction** – Iterates over scanned oil well PDFs, extracts text and tables with [pdfplumber](https://github.com/jsvine/pdfplumber), and parses well identifiers, coordinates, addresses, and stimulation (proppant/chemical) details.
- **Database** – Writes to SQLite (`oil_wells.db`): one `wells` row per PDF and one or more `stimulation_data` rows per well.
- **Web scraping** - For each well, put together a drillingedge.com URL from the API number, well name, and county. Then, it fetches and parses the page to collect well status, well type, closest city, and oil/gas production quantities.
- **Data preprocessing** - Cleans all database fields: strips HTML tags and special characters, normalizes missing values to N/A or 0, standardizes dates to ISO YYYY-MM-DD format, converts production shorthand (e.g. 1.5 k -> 1500), and validates coordinates to be within North Dakota boundaries.
- **Output** – Structured data ready for the map (Part 2) or other analysis.

## Requirements

- Python 3.8+
- Linux or macOS (or Windows with the same commands)

## Install

```bash
git clone https://github.com/dylanachen/oil-well-map-webpage.git
cd oil-well-map-webpage
python3 -m venv venv   # or: python3 -m venv .venv
source venv/bin/activate   # Windows: venv\Scripts\activate  (or .venv\Scripts\activate)
pip install -r requirements.txt
```

## Data

Place oil well PDFs in the `pdfs/` directory (same name as `pdf_extractor.py`'s default `--pdf-dir`). You can use the [assignment Drive folder](https://drive.google.com/drive/u/4/folders/12g-bhOylyaMoLF5djocnAeZHBx-gsxgY) or your own PDFs.

**Step 1: Extract PDF data**

## Run

```bash
source venv/bin/activate   # or: source .venv/bin/activate
python pdf_extractor.py
```

**Options:**

| Option | Default | Description |
|--------|---------|-------------|
| `--pdf-dir` | `pdfs` | Directory containing PDFs |
| `--db-path` | `oil_wells.db` | Output SQLite database path |
| `--max-pages` | (no limit) | Max pages to read per PDF |
| `--limit` | (all) | Process only the first N PDFs (e.g. `--limit 10`) |
| `--files` | (all) | Comma-separated PDF filenames (e.g. `W28651.pdf,W20197.pdf`) |

**Examples:**

```bash
python pdf_extractor.py --limit 10            # Test run: first 10 PDFs
python pdf_extractor.py --db-path ./wells.db  # Custom DB path
python pdf_extractor.py --files "W28651.pdf,W20197.pdf"  # Specific PDFs only
```

**Step 2: Scrape additional data from drillingedge.com**

The scraped fields from [drillingedge.com](drillingedge.com) are appended to their respective existing entries in the wells table. Progress is logged and saved per-well, so the script can be interrupted and resumed without issue.

```bash
python scrape_drillingedge.py
```

| Option | Default | Description |
|--------|---------|-------------|
| `--db` | `oil_wells.db` | SQLite database path |
| `--delay` | `1.0` | Seconds to wait between requests |
| `--max-wells` | (all) | Only scrape the first N wells |
| `--rescrape` | off | Re-scrape wells that were already fetched |

**Step 3: Preprocess and clean data**

Runs over both the `wells` and `stimulation_data` tables to clean every field. Includes a dry-run mode to preview changes before committing them. Prints a summary of the database state and missing value counts after cleaning.

```bash
python preprocess.py
```

| Option | Default | Description |
|--------|---------|-------------|
| `--db` | `oil_wells.db` | SQLite database path |
| `--dry-run` | off | Show what would change without writing |

**Preprocessing operations performed:**

- Strip HTML tags and non-printable/control characters from all text fields
- Normalize missing values: empty strings, null, none, -- -> N/A (text) or 0 (numeric)
- Standardize dates to ISO YYYY-MM-DD
- Validate and correct API number format (XX-XXX-XXXXX)
- Validate latitude/longitude against North Dakota boundaries (45.934-48.9982 deg N, 96.5671-104.0501 deg W)
- Convert production shorthand (1.5 k -> 1500)
- Replace NULL numeric stimulation fields with 0

## Database schema

**wells** – One row per PDF: `id`, `api_number`, `well_file_no`, `well_name`, `latitude`, `longitude`, `address`, `county`, `field`, `operator`, `permit_number`, `permit_date`, `total_depth`, `formation`, `stimulation_notes`, `raw_extract`, `pdf_source`, `created_at`, `well_status`, `well_type`, `closest_city`, `barrels_oil_produced`, `mcf_gas_produced`, `drillingedge_url`.

**stimulation_data** – Per-well stimulation records: `id`, `well_id`, `date_stimulated`, `stimulated_formation`, `top_ft`, `bottom_ft`, `stimulation_stages`, `volume`, `volume_units`, `type_treatment`, `acid_pct`, `lbs_proppant`, `max_treatment_pressure_psi`, `max_treatment_rate`, `details`.
