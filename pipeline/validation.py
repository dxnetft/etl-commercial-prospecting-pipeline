"""
Stage 3 — Validation: 9 checks on prospect data.
Each check returns a boolean mask (True = row has the issue).
"""
import re

import pandas as pd
from fuzzywuzzy import fuzz
import gender_guesser.detector as gender

from pipeline.config import (
    EMAIL_REGEX,
    FUZZY_DOMAIN_THRESHOLD,
    FUZZY_EMAIL_THRESHOLD,
    PRIVATE_DOMAINS,
)
from pipeline.utils import clean_company_text, detect_gender, normalize_url, normalize_name_for_email

_EMAIL_RE = re.compile(EMAIL_REGEX)


# ---------------------------------------------------------------------------
# Check 1: Bad email format
# ---------------------------------------------------------------------------
def check_email_format(df: pd.DataFrame) -> pd.Series:
    """True where email does not match the expected format."""
    return ~df["Email"].fillna("").astype(str).str.match(_EMAIL_RE)


# ---------------------------------------------------------------------------
# Check 2: Private email domain
# ---------------------------------------------------------------------------
def check_private_email(df: pd.DataFrame) -> pd.Series:
    """True where email domain is a known personal provider."""
    def _is_private(email: str) -> bool:
        if not isinstance(email, str) or "@" not in email:
            return False
        domain = email.split("@")[-1].lower().strip()
        return domain in PRIVATE_DOMAINS

    return df["Email"].apply(_is_private)


# ---------------------------------------------------------------------------
# Check 3: Bad names
# ---------------------------------------------------------------------------
def check_bad_names(df: pd.DataFrame) -> pd.Series:
    """
    Title-case First Name and Last Name in-place, then return mask
    where either is blank after cleaning.
    """
    for col in ("First Name", "Last Name"):
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str).str.strip().str.title()

    first_bad = df["First Name"].fillna("") == ""
    last_bad = df["Last Name"].fillna("") == ""
    return first_bad | last_bad


# ---------------------------------------------------------------------------
# Check 4: Fuzzy name–email mismatch
# ---------------------------------------------------------------------------
def check_fuzzy_email(df: pd.DataFrame) -> pd.Series:
    """True where neither first nor last name appears to match the email local part."""
    def _name_not_in_email(row) -> bool:
        email = str(row.get("Email", ""))
        if "@" not in email:
            return False
        local = email.split("@")[0].lower()
        first = normalize_name_for_email(str(row.get("First Name", "")))
        last = normalize_name_for_email(str(row.get("Last Name", "")))

        # Initials check
        if first and last:
            initials = first[0] + last[0]
            if initials in local:
                return False
            if first[0] in local and last[0] in local:
                return False

        # Fuzzy match
        if first and fuzz.partial_ratio(first, local) >= FUZZY_EMAIL_THRESHOLD:
            return False
        if last and fuzz.partial_ratio(last, local) >= FUZZY_EMAIL_THRESHOLD:
            return False
        return True

    return df.apply(_name_not_in_email, axis=1)


# ---------------------------------------------------------------------------
# Check 5: Fuzzy domain mismatch
# ---------------------------------------------------------------------------
def check_fuzzy_domain(df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    """
    Compare cleaned account name against email domain.
    Returns (bad_domain_mask, domain_score_series).
    Score is stored as a string like '35%' for display.
    """
    masks = []
    scores = []

    for _, row in df.iterrows():
        email = str(row.get("Email", ""))
        account = str(row.get("Account Name", ""))
        if "@" not in email or not account.strip():
            masks.append(False)
            scores.append("")
            continue

        domain_raw = email.split("@")[-1].lower()
        domain_core = domain_raw.split(".")[0]

        cleaned_account = clean_company_text(account)
        score = fuzz.partial_ratio(cleaned_account, domain_core)
        scores.append(f"{score}%")
        masks.append(score < FUZZY_DOMAIN_THRESHOLD)

    return pd.Series(masks, index=df.index), pd.Series(scores, index=df.index)


# ---------------------------------------------------------------------------
# Check 6: Bad LinkedIn URL
# ---------------------------------------------------------------------------
def check_linkedin(df: pd.DataFrame) -> pd.Series:
    """True where LinkedIn URL is filled but doesn't contain 'linkedin'."""
    if "LinkedIn URL" not in df.columns:
        return pd.Series(False, index=df.index)

    url = df["LinkedIn URL"].fillna("").astype(str).str.strip()
    filled = url != ""
    bad = ~url.str.lower().str.contains("linkedin", na=False)
    return filled & bad


# ---------------------------------------------------------------------------
# Check 7: Gender detection
# ---------------------------------------------------------------------------
def check_gender(
    df: pd.DataFrame, detector: gender.Detector
) -> tuple[pd.Series, pd.Series]:
    """
    Detect plausible gender from First Name.
    Returns (issue_mask, plausible_gender_series).
    Issue label: 'Bad Gender' if declared gender mismatches detected,
                 'Unknown Gender' if detection yields Unknown.
    """
    plausible = df["First Name"].apply(lambda n: detect_gender(str(n), detector))
    declared = df["Gender"].fillna("Unknown").astype(str).str.strip()

    def _issue(row_idx) -> bool:
        det = plausible.iloc[row_idx]
        dec = declared.iloc[row_idx]
        if det == "Unknown":
            return True  # Unknown gender flag
        if dec not in ("Male", "Female", "Unknown", ""):
            return True
        if dec in ("Male", "Female") and det != dec:
            return True
        return False

    mask = pd.Series(
        [_issue(i) for i in range(len(df))], index=df.index
    )
    return mask, plausible


# ---------------------------------------------------------------------------
# Check 8: Duplicate full names
# ---------------------------------------------------------------------------
def check_duplicates(df: pd.DataFrame) -> pd.Series:
    """
    True where full name (First + Last) is duplicated,
    including reversed order (Last, First) == (First, Last) of another row.
    """
    names = (
        df["First Name"].fillna("").str.strip().str.lower()
        + " "
        + df["Last Name"].fillna("").str.strip().str.lower()
    )
    names_rev = (
        df["Last Name"].fillna("").str.strip().str.lower()
        + " "
        + df["First Name"].fillna("").str.strip().str.lower()
    )

    all_names = pd.concat([names, names_rev])
    dup_values = all_names[all_names.duplicated(keep=False)].unique()
    return names.isin(dup_values) | names_rev.isin(dup_values)


# ---------------------------------------------------------------------------
# Check 9: Threshold (extra prospects per account)
# ---------------------------------------------------------------------------
def check_threshold(df: pd.DataFrame, threshold: int) -> pd.Series:
    """
    Returns a series: 'OK' for rows within threshold, 'Extra N' for excess rows.
    Counted per Account Name, in current sort order.
    """
    counts = df.groupby("Account Name", sort=False).cumcount()
    result = counts.apply(
        lambda n: "OK" if n < threshold else f"Extra {n - threshold + 1}"
    )
    return result


# ---------------------------------------------------------------------------
# Master entry point
# ---------------------------------------------------------------------------
def run_all_checks(
    df: pd.DataFrame,
    detector: gender.Detector,
    threshold: int,
) -> pd.DataFrame:
    """
    Apply all 9 checks to df.
    Adds columns: Issue, Domain Score, Plausible Gender, # Prospects/Account Range.
    'Issue' is a comma-separated string of all triggered check names.
    """
    df = df.copy()

    issues: dict[str, pd.Series] = {}

    # Check 1
    issues["Bad Email Format"] = check_email_format(df)
    # Check 2
    issues["Private Email"] = check_private_email(df)
    # Check 3 — modifies df First/Last Name in-place (title case)
    issues["Bad Names"] = check_bad_names(df)
    # Check 4
    issues["Bad Email (fuzzy)"] = check_fuzzy_email(df)
    # Check 5
    domain_mask, domain_scores = check_fuzzy_domain(df)
    issues["Bad Domain"] = domain_mask
    df["Domain Score"] = domain_scores
    # Check 6
    issues["Bad LinkedIn"] = check_linkedin(df)
    # Check 7
    gender_mask, plausible_gender = check_gender(df, detector)
    issues["Gender Issue"] = gender_mask
    df["Plausible Gender"] = plausible_gender
    # Check 8
    issues["Duplicate"] = check_duplicates(df)
    # Check 9
    df["# Prospects/Account Range"] = check_threshold(df, threshold)

    # Build Issue column
    def _build_issue(idx: int) -> str:
        labels = [name for name, mask in issues.items() if mask.iloc[idx]]
        return ", ".join(labels)

    df["Issue"] = [_build_issue(i) for i in range(len(df))]

    print(f"  Validation complete. {df['Issue'].ne('').sum()} rows flagged.")
    return df
