"""
Stage 1 — Account setup: load, validate, enrich, and clean accounts.
"""
from pathlib import Path

import pandas as pd

from pipeline.config import (
    ACCOUNTS_TEMPLATE_SHEET,
    ACCOUNTS_TEMPLATE_SKIPROWS,
    ACCOUNTS_VALID_ID_MIN,
)
from pipeline.utils import normalize_url, resolve_country


def load_wikidata_csv(path: Path) -> pd.DataFrame:
    """
    Load companies_wikidata.csv.
    Strips 'Q' prefix from QID column, converts to numeric Account ID.
    Returns DataFrame with columns: Account ID, Account Name, Website URL, Country
    """
    df = pd.read_csv(path, dtype=str)
    # Normalize column names (tolerate minor variations from Wikidata exports)
    df.columns = df.columns.str.strip()
    col_map = {
        "QID": "Account ID",
        "Company Name": "Account Name",
        "Website": "Website URL",
        "Country": "Country",
    }
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
    # Strip Q prefix and convert to numeric
    df["Account ID"] = (
        df["Account ID"].str.replace("Q", "", regex=False).str.strip()
    )
    df["Account ID"] = pd.to_numeric(df["Account ID"], errors="coerce")
    return df[["Account ID", "Account Name", "Website URL", "Country"]].copy()


def load_accounts_template(path: Path) -> pd.DataFrame:
    """
    Load accounts template XLSX.
    Sheet: ACCOUNTS_TEMPLATE_SHEET, skipping ACCOUNTS_TEMPLATE_SKIPROWS rows.
    Renames the verbose Website column to 'Website URL'.
    """
    df = pd.read_excel(
        path,
        sheet_name=ACCOUNTS_TEMPLATE_SHEET,
        header=0,
        skiprows=[1, 2, 3, 4],  # skip blank rows between header and data
    )
    # Rename the long column name
    df = df.rename(
        columns={"Website URL (if Account ID is not available)": "Website URL"}
    )
    df.columns = df.columns.str.strip()
    # Coerce Account ID to numeric
    if "Account ID" in df.columns:
        df["Account ID"] = pd.to_numeric(df["Account ID"], errors="coerce")
    return df


def validate_accounts(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """
    Run 4 structural checks. Prints warnings; does not raise.
    Returns dict with keys: duplicate_ids, duplicate_names, blank_names, dummy_rows.
    """
    issues: dict[str, pd.DataFrame] = {}

    valid_id_mask = df["Account ID"].notna() & (df["Account ID"] >= ACCOUNTS_VALID_ID_MIN)
    dup_ids = df[valid_id_mask & df.duplicated(subset=["Account ID"], keep=False)]
    issues["duplicate_ids"] = dup_ids
    if not dup_ids.empty:
        print(f"  ⚠  {len(dup_ids)} rows with duplicate Account IDs")

    dup_names = df[df["Account Name"].notna() & df.duplicated(subset=["Account Name"], keep=False)]
    issues["duplicate_names"] = dup_names
    if not dup_names.empty:
        print(f"  ⚠  {len(dup_names)} rows with duplicate Account Names")

    blank_names = df[df["Account Name"].isna() | (df["Account Name"].str.strip() == "")]
    issues["blank_names"] = blank_names
    if not blank_names.empty:
        print(f"  ⚠  {len(blank_names)} rows with blank Account Names")

    dummy_rows = df[df["Account Name"].str.lower().str.contains("dummy", na=False)]
    issues["dummy_rows"] = dummy_rows
    if not dummy_rows.empty:
        print(f"  ⚠  {len(dummy_rows)} dummy rows detected")

    return issues


def enrich_accounts(accounts: pd.DataFrame, crm: pd.DataFrame) -> pd.DataFrame:
    """
    Enrich accounts from CRM data.
    Pass 1: fill missing Website URL / Country by Account ID match (ID >= threshold).
    Pass 2: fill missing Country by normalized Website URL match.
    """
    df = accounts.copy()
    crm_clean = crm.copy()
    crm_clean["Website URL norm"] = crm_clean["Website URL"].apply(normalize_url)

    # Pass 1 — match by Account ID
    valid_crm = crm_clean[crm_clean["Account ID"] >= ACCOUNTS_VALID_ID_MIN].set_index("Account ID")
    for idx, row in df.iterrows():
        aid = row.get("Account ID")
        if pd.notna(aid) and aid >= ACCOUNTS_VALID_ID_MIN and aid in valid_crm.index:
            crm_row = valid_crm.loc[aid]
            if pd.isna(row.get("Website URL")) or str(row.get("Website URL")).strip() == "":
                df.at[idx, "Website URL"] = crm_row["Website URL"]
            if pd.isna(row.get("Country")) or str(row.get("Country")).strip() == "":
                df.at[idx, "Country"] = crm_row["Country"]

    # Pass 2 — match by normalized Website URL for rows still missing Account ID
    df["_website_norm"] = df["Website URL"].apply(normalize_url)
    crm_by_url = crm_clean.set_index("Website URL norm")
    for idx, row in df.iterrows():
        if pd.isna(row.get("Account ID")) or row.get("Account ID", 0) < ACCOUNTS_VALID_ID_MIN:
            norm = row["_website_norm"]
            if norm and norm in crm_by_url.index:
                crm_row = crm_by_url.loc[norm]
                if isinstance(crm_row, pd.DataFrame):
                    crm_row = crm_row.iloc[0]
                if pd.isna(row.get("Country")) or str(row.get("Country")).strip() == "":
                    df.at[idx, "Country"] = crm_row["Country"]
                if pd.isna(row.get("Account ID")) or row.get("Account ID", 0) < ACCOUNTS_VALID_ID_MIN:
                    df.at[idx, "Account ID"] = crm_row["Account ID"]

    df = df.drop(columns=["_website_norm"], errors="ignore")
    return df


def clean_accounts(df: pd.DataFrame) -> pd.DataFrame:
    """
    - Normalize Website URLs
    - Resolve 2-letter country codes to full names
    - Rename 'Account ID' → 'Custom Id'
    - Rename 'Assigned to' → 'Tags' if present
    - Sort by Account Name
    """
    df = df.copy()
    df["Website URL"] = df["Website URL"].apply(normalize_url)
    df["Country"] = df["Country"].apply(resolve_country)
    df = df.rename(columns={"Account ID": "Custom Id"})
    if "Assigned to" in df.columns:
        df = df.rename(columns={"Assigned to": "Tags"})
        df["Tags"] = df["Tags"].astype(str)
    df = df.sort_values("Account Name", na_position="last").reset_index(drop=True)
    return df


def build_accounts(wikidata_path: Path, template_path: Path) -> pd.DataFrame:
    """Orchestrate Stage 1. Returns final cleaned accounts DataFrame."""
    print("Loading Wikidata CRM data...")
    crm = load_wikidata_csv(wikidata_path)
    print(f"  {len(crm)} CRM companies loaded.")

    print("Loading accounts template...")
    accounts = load_accounts_template(template_path)
    print(f"  {len(accounts)} accounts in template.")

    print("Validating accounts...")
    validate_accounts(accounts)

    print("Enriching accounts from CRM...")
    accounts = enrich_accounts(accounts, crm)

    print("Cleaning accounts...")
    accounts = clean_accounts(accounts)

    print(f"  Done. {len(accounts)} accounts ready.")
    return accounts


def export_accounts(df: pd.DataFrame, output_path: Path) -> None:
    """Write accounts CSV (utf-8-sig for Excel compatibility)."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"  Accounts saved -> {output_path}")
