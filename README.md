# Commercial Prospecting ETL Pipeline

A modular Python ETL pipeline for B2B commercial prospecting that replaces proprietary paid data sources (SAP CRM, Outreach, ZoomInfo, LeadGenius) with free, publicly available alternatives. Built as part of a Bachelor's Engineering thesis.

## Overview

The pipeline ingests company and contact data from open sources, validates data quality through 9 automated checks, flags issues for manual review, and produces structured Excel and CSV deliverables ready for upload to outreach tools.

```
Wikidata CSV (companies)          Prospect CSV (contacts)
        |                                  |
        v                                  v
  [Stage 1]                          [Stage 2]
  Account Setup                   Prospect Ingestion
  - Enrich accounts                - Column mapping
  - Resolve countries              - Merge with accounts
  - Normalize URLs                 - Assign Prospect IDs
        |                                  |
        +----------------+-----------------+
                         |
                         v
                    [Stage 3]
                    Validation (9 checks)
                    - Email format
                    - Private email domains
                    - Blank names
                    - Name-email fuzzy match
                    - Domain-company fuzzy match
                    - LinkedIn URL validity
                    - Gender detection
                    - Duplicate names
                    - Threshold per account
                         |
                         v
                    [Stage 4]
                 Issues Export (Excel)
                 Manual fix by user
                 Reload + re-validate
                         |
                         v
                    [Stage 5]
                 Deliverable Export
                 - Prospects.xlsx (protected)
                 - Accounts without Prospects
                 - Prospect Upload template
                         |
                         v
                    [Stage 6]
                 CSV Export + Duplicate Report
```

## Data Sources

| Pipeline Role | Original (paid) | Replacement (free) |
|---|---|---|
| CRM / Accounts | SAP CRM export | [Wikidata SPARQL](https://query.wikidata.org) — pre-downloaded CSV |
| Prospects | ZoomInfo export | Generic CSV matching ZoomInfo schema |

No API keys required. All data sources are publicly accessible.

## Project Structure

```
TFG_REAL/
├── pipeline/
│   ├── config.py        # All constants: domains, legal suffixes, passwords, dropdowns
│   ├── accounts.py      # Stage 1: load, enrich, and clean accounts
│   ├── prospects.py     # Stage 2: ingest, column-map, merge accounts
│   ├── validation.py    # Stage 3: all 9 validation checks
│   ├── export.py        # Stages 4-6: issues file, deliverable XLSX, CSV export
│   └── utils.py         # Shared helpers: URL normalization, country lookup, gender detection
├── data/                # Input files (provided by user)
│   ├── companies_wikidata.csv       # Wikidata company export (CRM replacement)
│   ├── sample_accounts_template.xlsx
│   └── prospects_source2.csv        # Prospect contacts (ZoomInfo schema)
├── output/              # Generated files (created at runtime)
├── run.py               # CLI entry point
└── requirements.txt
```

## Setup

```bash
cd TFG_REAL
pip install -r requirements.txt
```

**Dependencies:**

| Library | Purpose |
|---|---|
| pandas | Data manipulation |
| openpyxl | Read Excel files |
| xlsxwriter | Write formatted Excel files |
| pycountry | ISO country code resolution |
| gender-guesser | First-name gender detection |
| fuzzywuzzy | Fuzzy string matching (name/domain checks) |
| python-Levenshtein | Speed up fuzzywuzzy |
| phonenumbers | Phone number parsing and E.164 formatting |

## Running the Pipeline

```bash
python run.py
```

You will be prompted for:

| Prompt | Default | Description |
|---|---|---|
| Output file prefix | `Output` | Label prepended to all output filenames |
| Wikidata CSV path | `data/companies_wikidata.csv` | Company CRM data |
| Accounts template path | `data/sample_accounts_template.xlsx` | Account list (XLSX, sheet "Template") |
| Prospect file path | `data/prospects_source2.csv` | Contact data CSV or XLSX |
| Prospects per account threshold | `5` | Max contacts per account before flagging extras |

Press **Enter** at any prompt to accept the default.

## Outputs

All files are written to the `output/` directory with your chosen prefix.

| File | Description |
|---|---|
| `accounts.csv` | Enriched accounts (used internally between stages) |
| `{prefix}_Prospect Issues.xlsx` | Flagged rows for manual review |
| `{prefix}_Source1 Prospects.xlsx` | Cleaned prospects (all columns) |
| `{prefix}_Deliverable.xlsx` | Final deliverable — 3 sheets, password-protected |
| `{prefix}_Source1 Prospects.csv` | Upload-ready CSV for outreach tools |

### Deliverable Excel sheets

- **Prospects** — All validated contacts. Locked except: Issue Category, Comments, Tags, Gender. Password: `prospecting`
- **Accounts without Prospects** — Accounts for which no valid contact was found
- **Prospect Upload** — Template with dropdowns (Gender, Source, Country) for manual additions

## Validation Checks

| # | Check | Flag |
|---|---|---|
| 1 | Email format | Does not match `name@domain.tld` pattern |
| 2 | Private email | Domain is gmail, yahoo, hotmail, etc. |
| 3 | Blank names | First Name or Last Name is empty |
| 4 | Name–email mismatch | Neither name appears in email local part (fuzzy, >75%) |
| 5 | Domain–company mismatch | Company name doesn't match email domain (fuzzy, <50%) |
| 6 | Bad LinkedIn URL | LinkedIn field filled but does not contain "linkedin" |
| 7 | Gender detection | Name-derived gender conflicts with declared gender, or unknown |
| 8 | Duplicate names | Same full name appears more than once (including reversed order) |
| 9 | Threshold exceeded | More contacts than allowed per account ("Extra N") |

## Accounts Template Format

The accounts XLSX must have:
- Sheet name: `Template`
- Header in row 1, data starting in row 6 (rows 2–5 are blank)
- Columns: `Account ID`, `Account Name`, `Website URL (if Account ID is not available)`, `Country`, `Assigned to`

## Wikidata CSV Format

Download from [query.wikidata.org](https://query.wikidata.org) using the SPARQL query in `WIKIDATA_QUERIES.md` (if present), or use the provided sample. Required columns:

| Column | Description |
|---|---|
| `QID` | Wikidata entity ID (e.g. Q312) |
| `Company Name` | Official company name |
| `Website` | Company domain |
| `Country` | ISO country name |

## Prospect CSV Format (ZoomInfo schema)

| Source column | Pipeline column |
|---|---|
| `ZoomInfo Contact ID` | `Contact ID` |
| `Email Address` | `Email` |
| `Job Title` | `Title` |
| `Direct Phone Number` | `Work Phone` |
| `Mobile phone` | `Mobile Phone` |
| `ZoomInfo Contact Profile URL` | `Source Evidence` |
| `LinkedIn Contact Profile URL` | `LinkedIn URL` |
| `Person City / State / Zip / Country` | `City / State / Zip / Country` |
| `ZoomInfo Company ID` | `Company ID` |
| `Company Name` | `Company` |
| `Custom Id` | `Custom Id` (account link) |
