# DSCI-560 Lab 6: Oil Wells Data Wrangling

This project extracts well-specific information and stimulation data from scanned oil well PDF documents and stores them in a database.

## Tasks Completed

- **Task 1**: Initial Setup – Python environment with PDF extraction tools
- **Task 2**: Data Collection/Storage – Database tables for wells and stimulation data
- **Task 3**: PDF Extraction – Python script to iterate over PDFs, extract data, and store in database tables

## Setup Requirements

### Prerequisites

- Python 3.8 or higher
- Linux environment (as required by the assignment)

### Installation Steps

1. **Create and activate virtual environment**:
   ```bash
   python3 -m venv venv
   source venv/bin/activate   # On Linux/macOS
   # On Windows: venv\Scripts\activate
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Ensure PDFs are in `pdfs/` folder**:
   - Download the Google Drive folder: https://drive.google.com/drive/u/4/folders/12g-bhOylyaMoLF5djocnAeZHBx-gsxgY
   - Place all PDF files in the `pdfs/` directory

### Running the Extraction Script

```bash
source venv/bin/activate
python pdf_extractor.py                       # Process all PDFs in pdfs/
python pdf_extractor.py --limit 10            # Process first 10 PDFs only (for testing)
python pdf_extractor.py --pdf-dir pdfs        # Specify a different PDFs directory
```

This will:
- Create/connect to `oil_wells.db` (SQLite by default)
- Iterate over all PDFs in `pdfs/`
- Extract API#, longitude, latitude, well name & number, address
- Extract stimulation data where available and store structured rows in `stimulation_data`
- Store results in database tables

### Database Schema

- **wells**: id, api_number, well_file_no, well_name, latitude, longitude, address, county, field, operator, permit_number, permit_date, total_depth, formation, stimulation_notes, raw_extract, pdf_source, created_at.
- **stimulation_data**: id, well_id, date_stimulated, stimulated_formation, top_ft, bottom_ft, stimulation_stages, volume, volume_units, type_treatment, acid_pct, lbs_proppant, max_treatment_pressure_psi, max_treatment_rate, details.

---

## Part 1: Compliance with assignment (word-for-word)

| Assignment requirement | Implementation |
|------------------------|----------------|
| **1) Initial Setup** – Use Python scripts and tools (OCRMYPDF, PyPDF, PyTesseract, etc.); document setup in submitted document | `pdf_extractor.py` uses **pdfplumber** and stdlib **sqlite3** (see `requirements.txt`). Setup and run instructions are in this README. Optional OCR (pytesseract/ocrmypdf) commented in `requirements.txt`. |
| **2) Data Collection / Storage** – Create database tables; parse PDFs; collect information; focus on oil well physical location and specifications | Script creates **wells** and **stimulation_data** tables; parses each PDF under `pdfs/`; stores API#, coordinates, well name/number, address, and related fields. |
| **3) PDF Extraction** – Download folder from Drive; write Python script to **iterate over all PDFs** in folder; **extract** information; **store in database tables**; PDFs have well-specific info and **stimulation data** (proppant/chemical injected) | Script uses `Path(pdf_dir).glob('*.pdf')` to iterate all PDFs; extracts text/tables via pdfplumber; inserts into SQLite `oil_wells.db`. Stimulation records stored in `stimulation_data`. |
| **Figure 1** – Relevant data: **API#**, **longitude**, **latitude**, **well name & number**, **address**, and any relevant fields | Extracted and stored: `api_number`, `longitude`, `latitude`, `well_name`, `well_file_no` (well number), `address`; plus `county`, `field`, `operator`, `permit_number`, `permit_date`, `total_depth`, `formation`, `stimulation_notes`. |
| **Figure 2** – Stimulation data: **extract all fields mentioned in the snapshot** | All standard stimulation fields extracted and stored in `stimulation_data`: **date_stimulated**, **stimulated_formation**, **top_ft**, **bottom_ft**, **stimulation_stages**, **volume**, **volume_units**, **type_treatment**, **acid_pct**, **lbs_proppant**, **max_treatment_pressure_psi**, **max_treatment_rate**, **details**. |
| **5) Data Preprocessing** – Remove HTML tags, special characters, irrelevant info; transform to suitable format (e.g. timestamps); replace missing data with 0 or N/A | `clean_value()` strips HTML tags and control characters. Missing well text fields (well name, address, county, etc.) are set to **N/A** before storing. Stimulation rows store extracted values as-is (NULL where missing). |
