"""
Stages 4, 5, 6 — Export: issues file, deliverable XLSX, CSV export.
"""
from pathlib import Path

import pandas as pd

from pipeline.config import (
    COUNTRY_OPTIONS,
    ERROR_CATEGORY_OPTIONS,
    GENDER_OPTIONS,
    SHEET_PASSWORD,
    SOURCE_LABEL,
)

# ---------------------------------------------------------------------------
# Working columns added during validation (stripped from deliverable)
# ---------------------------------------------------------------------------
_WORKING_COLS = ["Issue", "Domain Score", "Plausible Gender", "# Prospects/Account Range"]


# ---------------------------------------------------------------------------
# Stage 4: Issues file
# ---------------------------------------------------------------------------
def export_issues_file(df: pd.DataFrame, output_path: Path) -> None:
    """
    Write _Prospect Issues.xlsx with autofilter and formatted header.
    Flagged rows (non-empty Issue) are highlighted in light red.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Column order: working cols first, then the rest
    priority_cols = ["Issue", "# Prospects/Account Range", "Prospect ID", "Account Name",
                     "First Name", "Last Name", "Title", "Gender", "Plausible Gender",
                     "Email", "Domain Score", "LinkedIn URL", "Work Phone", "Mobile Phone",
                     "City", "State", "Zip", "Country", "Source"]
    present_priority = [c for c in priority_cols if c in df.columns]
    remaining = [c for c in df.columns if c not in present_priority]
    col_order = present_priority + remaining

    out = df[col_order].copy()

    with pd.ExcelWriter(str(output_path), engine="xlsxwriter") as writer:
        out.to_excel(writer, sheet_name="Issues", index=False)
        wb = writer.book
        ws = writer.sheets["Issues"]

        n_rows, n_cols = out.shape

        # Formats
        header_fmt = wb.add_format({
            "bold": True, "bg_color": "#1F4E79", "font_color": "#FFFFFF",
            "border": 1, "align": "center",
        })
        flagged_fmt = wb.add_format({"bg_color": "#FFE0E0"})
        normal_fmt = wb.add_format({})

        # Write header with custom format
        for col_idx, col_name in enumerate(out.columns):
            ws.write(0, col_idx, col_name, header_fmt)

        # Highlight flagged rows
        issue_col_idx = list(out.columns).index("Issue") if "Issue" in out.columns else -1
        for row_idx in range(n_rows):
            issue_val = out.iloc[row_idx].get("Issue", "") if "Issue" in out.columns else ""
            fmt = flagged_fmt if issue_val else normal_fmt
            for col_idx in range(n_cols):
                raw = out.iloc[row_idx, col_idx]
                # Convert NaN/NaT to empty string for xlsxwriter
                import math
                if raw is None or (isinstance(raw, float) and math.isnan(raw)):
                    raw = ""
                ws.write(row_idx + 1, col_idx, raw, fmt)

        # Autofilter
        ws.autofilter(0, 0, n_rows, n_cols - 1)

        # Autofit column widths (approx)
        for col_idx, col_name in enumerate(out.columns):
            max_len = max(len(str(col_name)), out.iloc[:, col_idx].astype(str).str.len().max())
            ws.set_column(col_idx, col_idx, min(max_len + 2, 50))

    print(f"  Issues file -> {output_path}  ({df['Issue'].ne('').sum()} flagged rows)")


def reload_fixed_issues(path: Path) -> pd.DataFrame:
    """Read the user-edited issues file back in."""
    return pd.read_excel(path, sheet_name="Issues")


# ---------------------------------------------------------------------------
# Stage 5: Deliverable XLSX
# ---------------------------------------------------------------------------
def build_deliverable_df(prospects: pd.DataFrame) -> pd.DataFrame:
    """
    Drop working columns; prepend Issue Category and Comments.
    """
    df = prospects.drop(columns=[c for c in _WORKING_COLS if c in prospects.columns])
    df.insert(0, "Issue Category", "")
    df.insert(1, "Comments", "")
    return df


def export_deliverable(
    deliverable_df: pd.DataFrame,
    accounts: pd.DataFrame,
    output_path: Path,
) -> None:
    """
    Write _Deliverable.xlsx with three sheets:
    - Prospects: password-protected, editable cols: Issue Category, Comments, Tags, Gender
    - Accounts without Prospects
    - Prospect Upload (template with dropdowns)
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Accounts without any prospects in deliverable
    accounts_with = set(deliverable_df["Account Name"].dropna().unique())
    accounts_without = accounts[~accounts["Account Name"].isin(accounts_with)].copy()

    # Prospect Upload template columns
    upload_cols = [
        "Custom Id", "Account Name", "First Name", "Last Name", "Title",
        "Gender", "Email", "Work Phone", "Mobile Phone", "LinkedIn URL",
        "City", "State", "Zip", "Country", "Source",
    ]

    with pd.ExcelWriter(str(output_path), engine="xlsxwriter") as writer:
        wb = writer.book

        # --- Sheet 1: Prospects ---
        deliverable_df.to_excel(writer, sheet_name="Prospects", index=False)
        ws_p = writer.sheets["Prospects"]
        n_rows, n_cols = deliverable_df.shape

        # Formats
        locked_fmt = wb.add_format({"locked": True, "bg_color": "#F2F2F2"})
        unlocked_fmt = wb.add_format({"locked": False, "bg_color": "#FFFFFF"})
        header_fmt = wb.add_format({
            "bold": True, "bg_color": "#1F4E79", "font_color": "#FFFFFF",
            "border": 1, "locked": True,
        })

        # Write header
        for col_idx, col_name in enumerate(deliverable_df.columns):
            ws_p.write(0, col_idx, col_name, header_fmt)

        # Editable columns
        editable = {"Issue Category", "Comments", "Tags", "Gender"}
        editable_indices = {
            i for i, c in enumerate(deliverable_df.columns) if c in editable
        }

        for row_idx in range(n_rows):
            for col_idx in range(n_cols):
                import math
                raw = deliverable_df.iloc[row_idx, col_idx]
                if raw is None or (isinstance(raw, float) and math.isnan(raw)):
                    raw = ""
                fmt = unlocked_fmt if col_idx in editable_indices else locked_fmt
                ws_p.write(row_idx + 1, col_idx, raw, fmt)

        # Issue Category dropdown
        if "Issue Category" in deliverable_df.columns:
            ic_idx = list(deliverable_df.columns).index("Issue Category")
            ws_p.data_validation(
                1, ic_idx, n_rows, ic_idx,
                {"validate": "list", "source": ERROR_CATEGORY_OPTIONS},
            )

        # Gender dropdown
        if "Gender" in deliverable_df.columns:
            g_idx = list(deliverable_df.columns).index("Gender")
            ws_p.data_validation(
                1, g_idx, n_rows, g_idx,
                {"validate": "list", "source": GENDER_OPTIONS},
            )

        # Protect sheet (unlocked cells remain editable)
        ws_p.protect(SHEET_PASSWORD, {
            "select_locked_cells": True,
            "select_unlocked_cells": True,
            "format_cells": False,
            "insert_rows": False,
            "delete_rows": False,
        })

        # --- Sheet 2: Accounts without Prospects ---
        accounts_without.to_excel(writer, sheet_name="Accounts without Prospects", index=False)
        ws_a = writer.sheets["Accounts without Prospects"]
        for col_idx, col_name in enumerate(accounts_without.columns):
            ws_a.set_column(col_idx, col_idx, max(len(str(col_name)) + 2, 15))

        # --- Sheet 3: Prospect Upload (template) ---
        # Instruction rows
        ws_u = wb.add_worksheet("Prospect Upload")
        instruction_fmt = wb.add_format({"italic": True, "font_color": "#888888"})
        instructions = [
            "Prospect Upload Template",
            "Fill in the columns below to upload prospects to the outreach tool.",
            "Required: Custom Id OR Account Name, Email OR Work Phone.",
            "Required fields: First Name, Last Name, Title, Gender, Country.",
            "",
            "",
        ]
        for r, text in enumerate(instructions):
            ws_u.write(r, 0, text, instruction_fmt)

        # Header row at row 6 (0-indexed)
        header_row = 6
        black_fmt = wb.add_format({
            "bold": True, "bg_color": "#000000", "font_color": "#FFFFFF",
        })
        for col_idx, col_name in enumerate(upload_cols):
            ws_u.write(header_row, col_idx, col_name, black_fmt)

        # Dropdowns in data area
        upload_data_start = header_row + 1
        upload_data_end = header_row + 1000

        gender_col = upload_cols.index("Gender")
        ws_u.data_validation(
            upload_data_start, gender_col, upload_data_end, gender_col,
            {"validate": "list", "source": GENDER_OPTIONS},
        )

        source_col = upload_cols.index("Source")
        ws_u.data_validation(
            upload_data_start, source_col, upload_data_end, source_col,
            {"validate": "list", "source": [SOURCE_LABEL]},
        )

        # Country dropdown via hidden sheet to stay under Excel's 255-char limit
        ws_dd = wb.add_worksheet("DropdownLists")
        ws_dd.hide()
        for r, country in enumerate(COUNTRY_OPTIONS):
            ws_dd.write(r, 0, country)
        n_countries = len(COUNTRY_OPTIONS)
        country_col = upload_cols.index("Country")
        ws_u.data_validation(
            upload_data_start, country_col, upload_data_end, country_col,
            {"validate": "list", "source": f"=DropdownLists!$A$1:$A${n_countries}"},
        )

    print(f"  Deliverable -> {output_path}")


def compute_statistics(
    accounts: pd.DataFrame,
    deliverable_df: pd.DataFrame,
    threshold: int,
) -> dict:
    """Compute pipeline statistics."""
    total_accounts = len(accounts)
    total_prospects = len(deliverable_df)
    prospects_with_email = deliverable_df["Email"].notna().sum()
    prospects_with_linkedin = (
        deliverable_df["LinkedIn URL"].notna()
        & (deliverable_df["LinkedIn URL"].str.strip() != "")
    ).sum() if "LinkedIn URL" in deliverable_df.columns else 0

    accounts_with = set(deliverable_df["Account Name"].dropna().unique())
    accounts_without = total_accounts - len(accounts_with)
    enrichment_rate = len(accounts_with) / total_accounts * 100 if total_accounts else 0

    per_account = deliverable_df.groupby("Account Name").size()
    accounts_few = (per_account < threshold).sum()
    accounts_sufficient = (per_account >= threshold).sum()

    return {
        "accounts_submitted": total_accounts,
        "prospects_found": total_prospects,
        "prospects_with_email": int(prospects_with_email),
        "prospects_with_linkedin": int(prospects_with_linkedin),
        "accounts_with_prospects": len(accounts_with),
        "accounts_without_prospects": accounts_without,
        "enrichment_rate": round(enrichment_rate, 1),
        "accounts_with_few": int(accounts_few),
        "accounts_sufficient": int(accounts_sufficient),
    }


def print_statistics(stats: dict, threshold: int) -> None:
    print("\n" + "=" * 50)
    print("PIPELINE STATISTICS")
    print("=" * 50)
    print(f"  Accounts submitted     : {stats['accounts_submitted']}")
    print(f"  Prospects found        : {stats['prospects_found']}")
    print(f"  Prospects with email   : {stats['prospects_with_email']}")
    print(f"  Prospects with LinkedIn: {stats['prospects_with_linkedin']}")
    print(f"  Accounts with prospects: {stats['accounts_with_prospects']}")
    print(f"  Accounts without       : {stats['accounts_without_prospects']}")
    print(f"  Enrichment rate        : {stats['enrichment_rate']}%")
    print(f"  Accounts >= {threshold} prospects : {stats['accounts_sufficient']}")
    print(f"  Accounts < {threshold} prospects  : {stats['accounts_with_few']}")
    print("=" * 50 + "\n")


# ---------------------------------------------------------------------------
# Stage 6: CSV export
# ---------------------------------------------------------------------------
def export_source_csv(
    source_xlsx_path: Path,
    deliverable_path: Path,
    output_path: Path,
) -> None:
    """
    Load per-source Prospects XLSX, merge Tags from Deliverable by Prospect ID,
    rename Custom Id -> Account custom ID, export CSV.
    """
    source_df = pd.read_excel(source_xlsx_path)
    deliverable_df = pd.read_excel(deliverable_path, sheet_name="Prospects")

    # Merge Tags from deliverable
    if "Tags" in deliverable_df.columns and "Prospect ID" in deliverable_df.columns:
        tags = deliverable_df[["Prospect ID", "Tags"]].drop_duplicates("Prospect ID")
        if "Tags" in source_df.columns:
            source_df = source_df.drop(columns=["Tags"])
        source_df = source_df.merge(tags, on="Prospect ID", how="left")

    # Rename columns for outreach upload
    source_df = source_df.rename(columns={"Custom Id": "Account custom ID"})
    if "Account Name" in source_df.columns and "Company" not in source_df.columns:
        source_df = source_df.rename(columns={"Account Name": "Company"})

    # Convert Account custom ID to int where possible
    if "Account custom ID" in source_df.columns:
        source_df["Account custom ID"] = pd.to_numeric(
            source_df["Account custom ID"], errors="coerce"
        ).astype("Int64")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    source_df.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"  CSV export -> {output_path}")


def run_duplicate_report(df: pd.DataFrame) -> None:
    """
    4-level duplicate detection report printed to stdout.
    Normalizes empty/nan strings before comparison.
    """
    MISSING = {"", "nan", "none", "null", "n/a", "na", "-", "x"}

    def _norm(val) -> str:
        s = str(val).strip().lower()
        return "" if s in MISSING else s

    email = df["Email"].apply(_norm)
    first = df["First Name"].apply(_norm) if "First Name" in df.columns else pd.Series("", index=df.index)
    last = df["Last Name"].apply(_norm) if "Last Name" in df.columns else pd.Series("", index=df.index)
    account = df["Account Name"].apply(_norm) if "Account Name" in df.columns else pd.Series("", index=df.index)

    print("\n--- Duplicate Report ---")

    # Level 1: global email dups
    dup1 = df[email.ne("") & email.duplicated(keep=False)]
    print(f"  Level 1 (global email duplicates)        : {len(dup1)} rows")

    # Level 2: email within account
    key2 = account + "||" + email
    dup2 = df[email.ne("") & key2.duplicated(keep=False)]
    print(f"  Level 2 (email dup within account)       : {len(dup2)} rows")

    # Level 3: account + first + last + email
    key3 = account + "||" + first + "||" + last + "||" + email
    dup3 = df[key3.ne("||||") & key3.duplicated(keep=False)]
    print(f"  Level 3 (account+name+email duplicate)   : {len(dup3)} rows")

    # Level 4: account + first + last (differing emails)
    key4 = account + "||" + first + "||" + last
    dup4_mask = key4.ne("||") & key4.duplicated(keep=False)
    dup4 = df[dup4_mask]
    print(f"  Level 4 (account+name, different emails) : {len(dup4)} rows")

    if len(dup1) == 0 and len(dup2) == 0 and len(dup3) == 0 and len(dup4) == 0:
        print("  No duplicates found.")
    print("--- End Duplicate Report ---\n")
