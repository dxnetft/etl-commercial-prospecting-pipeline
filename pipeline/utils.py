"""
Shared, stateless helper functions.
"""
import re
import unicodedata

import pandas as pd
import pycountry
import gender_guesser.detector as gender

from pipeline.config import LEGAL_SUFFIXES

# ---------------------------------------------------------------------------
# Legal suffix regex — compiled once at module load
# ---------------------------------------------------------------------------
_SUFFIX_PATTERN = re.compile(
    r"\b(" + "|".join(re.escape(s) for s in sorted(LEGAL_SUFFIXES, key=len, reverse=True)) + r")\b",
    re.IGNORECASE,
)


def normalize_url(url: str) -> str:
    """Lowercase, strip protocol and www, return bare domain."""
    if not isinstance(url, str) or not url.strip():
        return ""
    url = url.strip().lower()
    url = re.sub(r"^https?://", "", url)
    url = re.sub(r"^www\.", "", url)
    url = url.split("/")[0]
    return url


def resolve_country(name: str) -> str:
    """ISO alpha-2 code → full country name.  Non-code strings returned as-is (title case)."""
    if not isinstance(name, str) or not name.strip():
        return ""
    name = name.strip()
    if len(name) == 2:
        try:
            return pycountry.countries.get(alpha_2=name.upper()).name
        except AttributeError:
            pass
    return name.title()


def country_name_to_code(name: str) -> str:
    """Full country name → ISO alpha-2 code.  Returns original string if not found."""
    if not isinstance(name, str) or not name.strip():
        return ""
    name = name.strip()
    try:
        return pycountry.countries.lookup(name).alpha_2
    except LookupError:
        return name


def clean_company_text(text: str) -> str:
    """Strip legal suffixes and non-alphanumeric characters for fuzzy domain comparison."""
    if not isinstance(text, str):
        return ""
    text = _SUFFIX_PATTERN.sub(" ", text)
    text = re.sub(r"[^a-zA-Z0-9\s]", " ", text)
    return re.sub(r"\s+", " ", text).strip().lower()


def detect_gender(first_name: str, detector: gender.Detector) -> str:
    """
    Detect gender from first name.  Handles hyphenated names (uses first part).
    Returns 'Male', 'Female', or 'Unknown'.
    """
    if not isinstance(first_name, str) or not first_name.strip():
        return "Unknown"
    name = first_name.strip().split("-")[0].split(" ")[0]
    result = detector.get_gender(name)
    if result in ("male", "mostly_male"):
        return "Male"
    if result in ("female", "mostly_female"):
        return "Female"
    return "Unknown"


def assign_prospect_ids(df: pd.DataFrame, prefix: str) -> pd.DataFrame:
    """
    Sort by Account Name → Last Name → First Name and assign IDs like Z0001.
    Returns df with 'Prospect ID' column set.
    """
    df = df.copy()
    sort_cols = [c for c in ["Account Name", "Last Name", "First Name"] if c in df.columns]
    df = df.sort_values(sort_cols).reset_index(drop=True)
    df["Prospect ID"] = [f"{prefix}{str(i + 1).zfill(4)}" for i in range(len(df))]
    return df


def normalize_name_for_email(name: str) -> str:
    """Normalize accented characters to ASCII for fuzzy name-email matching."""
    if not isinstance(name, str):
        return ""
    nfkd = unicodedata.normalize("NFKD", name)
    return "".join(c for c in nfkd if not unicodedata.combining(c)).lower()
