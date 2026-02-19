# oil-well-map-webpage

Apache webpage serving an interactive map of scraped oil well data. This repo includes the data pipeline: PDF extraction and SQLite database of well information and stimulation data.

## What it does

- **PDF extraction** – Iterates over scanned oil well PDFs, extracts text and tables with [pdfplumber](https://github.com/jsvine/pdfplumber), and parses well identifiers, coordinates, addresses, and stimulation (proppant/chemical) details.
- **Database** – Writes to SQLite (`oil_wells.db`): one `wells` row per PDF and one or more `stimulation_data` rows per well.
- **Output** – Structured data ready for the map (Part 2) or other analysis.

## Requirements

- Python 3.8+
- Linux or macOS (or Windows with the same commands)

## Install

```bash
git clone https://github.com/dylanachen/oil-well-map-webpage.git
cd oil-well-map-webpage
python3 -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

## Data

Place oil well PDFs in the `pdfs/` directory. You can use the [assignment Drive folder](https://drive.google.com/drive/u/4/folders/12g-bhOylyaMoLF5djocnAeZHBx-gsxgY) or your own PDFs.

## Run

```bash
source venv/bin/activate
python pdf_extractor.py
```

**Options:**

| Option | Default | Description |
|--------|---------|-------------|
| `--pdf-dir` | `pdfs` | Directory containing PDFs |
| `--db-path` | `oil_wells.db` | Output SQLite database path |
| `--max-pages` | `300` | Max pages to read per PDF |
| `--limit` | (all) | Process only the first N PDFs (e.g. `--limit 10`) |

**Examples:**

```bash
python pdf_extractor.py --limit 10            # Test run: first 10 PDFs
python pdf_extractor.py --db-path ./wells.db  # Custom DB path
```

## Database schema

**wells** – One row per PDF: `id`, `api_number`, `well_file_no`, `well_name`, `latitude`, `longitude`, `address`, `county`, `field`, `operator`, `permit_number`, `permit_date`, `total_depth`, `formation`, `stimulation_notes`, `raw_extract`, `pdf_source`, `created_at`.

**stimulation_data** – Per-well stimulation records: `id`, `well_id`, `date_stimulated`, `stimulated_formation`, `top_ft`, `bottom_ft`, `stimulation_stages`, `volume`, `volume_units`, `type_treatment`, `acid_pct`, `lbs_proppant`, `max_treatment_pressure_psi`, `max_treatment_rate`, `details`.

## License

See repository license.
