"""
Stage 2 — Prospect ingestion: load, remap columns, merge accounts, assign IDs.
"""
from pathlib import Path

import pandas as pd

from pipeline.config import PROSPECT_ID_PREFIX, SOURCE_LABEL
from pipeline.utils import assign_prospect_ids

# ---------------------------------------------------------------------------
# Column mapping for ZoomInfo-style export
# ---------------------------------------------------------------------------
COLUMN_MAP: dict[str, str] = {
    "ZoomInfo Contact ID": "Contact ID",
    "Email Address": "Email",
    "Job Title": "Title",
    "Direct Phone Number": "Work Phone",
    "Mobile phone": "Mobile Phone",
    "ZoomInfo Contact Profile URL": "Source Evidence",
    "LinkedIn Contact Profile URL": "LinkedIn URL",
    "Person Street": "Address",
    "Person City": "City",
    "Person State": "State",
    "Person Zip Code": "Zip",
    "Person Country": "Country",
    "ZoomInfo Company ID": "Company ID",
    "Company Name": "Company",
    # "Custom Id" — already correct in the sample data
}

# Canonical columns retained after ingestion
CANONICAL_COLUMNS = [
    "Custom Id", "Account Name", "Tags",
    "Contact ID", "Company ID", "Company",
    "First Name", "Last Name", "Middle Name",
    "Gender", "Title",
    "Email", "Work Phone", "Mobile Phone",
    "Source Evidence", "LinkedIn URL",
    "Address", "City", "State", "Zip", "Country",
    "Company Founded", "Company SIC Code", "Company NAICS Code",
    "Source", "Prospect ID",
]


def load_prospects(path: Path) -> pd.DataFrame:
    """
    Auto-detect CSV or XLSX and load the prospect file.
    Applies COLUMN_MAP to rename source columns to generic names.
    """
    suffix = path.suffix.lower()
    if suffix in (".xlsx", ".xls"):
        df = pd.read_excel(path)
    else:
        df = pd.read_csv(path, dtype=str)

    df.columns = df.columns.str.strip()
    df = df.rename(columns={k: v for k, v in COLUMN_MAP.items() if k in df.columns})

    # Coerce numeric columns
    for col in ("Custom Id", "Zip", "Company ID", "Contact ID"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def filter_prospects(df: pd.DataFrame) -> pd.DataFrame:
    """Drop rows missing Email or Custom Id."""
    before = len(df)
    df = df.dropna(subset=["Email"])
    df = df[df["Email"].str.strip() != ""]
    df = df.dropna(subset=["Custom Id"])
    dropped = before - len(df)
    if dropped:
        print(f"  Dropped {dropped} rows (missing Email or Account ID).")
    return df.reset_index(drop=True)


def merge_accounts(prospects: pd.DataFrame, accounts: pd.DataFrame) -> pd.DataFrame:
    """
    Left-join prospects with accounts on Custom Id.
    Overwrites Account Name and Tags columns from the accounts table.
    """
    accounts_sub = accounts[["Custom Id", "Account Name", "Tags", "Country"]].copy()
    accounts_sub = accounts_sub.rename(columns={
        "Country": "Account Country",
        "Tags": "Tags",
    })

    # Drop existing Account Name / Tags from prospects to avoid collisions
    drop_cols = [c for c in ["Account Name", "Tags", "Account Country"] if c in prospects.columns]
    prospects = prospects.drop(columns=drop_cols)

    merged = prospects.merge(accounts_sub, on="Custom Id", how="left")
    return merged


def apply_source_label(df: pd.DataFrame, label: str = SOURCE_LABEL) -> pd.DataFrame:
    """Set the Source column."""
    df = df.copy()
    df["Source"] = label
    return df


def ingest(prospect_path: Path, accounts_path: Path) -> pd.DataFrame:
    """
    Full Stage 2 orchestrator.
    Returns a DataFrame with Prospect IDs assigned, ready for validation.
    """
    print(f"Loading prospects from {prospect_path.name}...")
    df = load_prospects(prospect_path)
    print(f"  {len(df)} rows loaded.")

    df = filter_prospects(df)

    print("Merging with accounts data...")
    accounts = pd.read_csv(accounts_path, encoding="utf-8-sig")
    accounts["Custom Id"] = pd.to_numeric(accounts["Custom Id"], errors="coerce")
    df = merge_accounts(df, accounts)

    df = apply_source_label(df)

    # Add Gender column if not present
    if "Gender" not in df.columns:
        df["Gender"] = "Unknown"

    print("Assigning Prospect IDs...")
    df = assign_prospect_ids(df, PROSPECT_ID_PREFIX)

    print(f"  {len(df)} prospects ingested.")
    return df
