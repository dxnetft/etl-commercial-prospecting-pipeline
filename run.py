"""
ETL Pipeline for Commercial Prospecting
Run: python run.py
"""
from pathlib import Path

import gender_guesser.detector as gender

from pipeline.accounts import build_accounts, export_accounts
from pipeline.prospects import ingest
from pipeline.validation import run_all_checks
from pipeline.export import (
    build_deliverable_df,
    compute_statistics,
    export_deliverable,
    export_issues_file,
    export_source_csv,
    print_statistics,
    reload_fixed_issues,
    run_duplicate_report,
)
from pipeline.utils import assign_prospect_ids
from pipeline.config import PROSPECT_ID_PREFIX, OUTPUT_DIR


def prompt_path(label: str, default: str) -> Path:
    val = input(f"{label} [{default}]: ").strip()
    return Path(val if val else default)


def prompt_int(label: str, default: int) -> int:
    val = input(f"{label} [{default}]: ").strip()
    try:
        return int(val) if val else default
    except ValueError:
        return default


def main() -> None:
    print("\n" + "=" * 60)
    print("  Commercial Prospecting ETL Pipeline")
    print("=" * 60 + "\n")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # --- Base name for output files ---
    base_name = input("Output file prefix (e.g. TestRun_2026): ").strip()
    if not base_name:
        base_name = "Output"

    # -------------------------------------------------------------------------
    # Stage 1: Account Setup
    # -------------------------------------------------------------------------
    print("\n[Stage 1] Account Setup")
    wikidata_path = prompt_path("  Wikidata CSV", "data/companies_wikidata.csv")
    template_path = prompt_path("  Accounts template XLSX", "data/sample_accounts_template.xlsx")

    accounts = build_accounts(wikidata_path, template_path)
    accounts_csv = OUTPUT_DIR / "accounts.csv"
    export_accounts(accounts, accounts_csv)

    # -------------------------------------------------------------------------
    # Stage 2: Prospect Ingestion
    # -------------------------------------------------------------------------
    print("\n[Stage 2] Prospect Ingestion")
    prospect_path = prompt_path("  Prospect file (CSV or XLSX)", "data/prospects_source2.csv")
    threshold = prompt_int("  Max prospects per account (threshold)", 5)

    prospects = ingest(prospect_path, accounts_csv)

    # -------------------------------------------------------------------------
    # Stage 3: Validation — Pass 1
    # -------------------------------------------------------------------------
    print("\n[Stage 3] Validation")
    detector = gender.Detector()
    prospects_validated = run_all_checks(prospects, detector, threshold)

    # -------------------------------------------------------------------------
    # Stage 4: Issues Export + Manual Fix Loop
    # -------------------------------------------------------------------------
    issues_path = OUTPUT_DIR / f"{base_name}_Prospect Issues.xlsx"
    export_issues_file(prospects_validated, issues_path)

    flagged = prospects_validated["Issue"].ne("").sum()
    if flagged > 0:
        print(f"\n  {flagged} rows flagged. Open the issues file, fix them, then press Enter.")
        print(f"  File: {issues_path}\n")
        input("  Press Enter when done (or Enter to skip fixes)...")

        # Reload fixed file and re-validate
        print("  Reloading fixed issues file...")
        try:
            fixed_df = reload_fixed_issues(issues_path)
            # Re-assign Prospect IDs after manual edits
            fixed_df = assign_prospect_ids(fixed_df, PROSPECT_ID_PREFIX)
            # Re-run validation on fixed data
            prospects_validated = run_all_checks(fixed_df, detector, threshold)
        except Exception as e:
            print(f"  Could not reload issues file ({e}). Using pre-fix data.")
    else:
        print("  No issues found. Proceeding with all prospects.")

    # -------------------------------------------------------------------------
    # Stage 5: Deliverable Export
    # -------------------------------------------------------------------------
    print("\n[Stage 5] Deliverable Export")
    deliverable_df = build_deliverable_df(prospects_validated)
    source_xlsx = OUTPUT_DIR / f"{base_name}_Source1 Prospects.xlsx"
    deliverable_xlsx = OUTPUT_DIR / f"{base_name}_Deliverable.xlsx"

    # Save source-specific XLSX
    deliverable_df.to_excel(str(source_xlsx), index=False)
    print(f"  Source XLSX -> {source_xlsx}")

    # Save full deliverable
    export_deliverable(deliverable_df, accounts, deliverable_xlsx)

    # Statistics
    stats = compute_statistics(accounts, deliverable_df, threshold)
    print_statistics(stats, threshold)

    # -------------------------------------------------------------------------
    # Stage 6: CSV Export
    # -------------------------------------------------------------------------
    print("[Stage 6] CSV Export")
    csv_path = OUTPUT_DIR / f"{base_name}_Source1 Prospects.csv"
    export_source_csv(source_xlsx, deliverable_xlsx, csv_path)

    # Duplicate report
    run_duplicate_report(deliverable_df)

    print(f"Pipeline complete. All outputs in: {OUTPUT_DIR}\n")


if __name__ == "__main__":
    main()
