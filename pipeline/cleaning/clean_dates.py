import pandas as pd
import re
from typing import Optional
from dateutil import parser as dateutil_parser


def normalize_date(date_str: Optional[str]) -> Optional[str]:
    """
    Converts ANY date format into standard ISO format YYYY-MM-DD.

    Our medications_log.json has dates in 10+ different formats:
    - "August 31, 2023"     -> "2023-08-31"
    - "08/04/2022"          -> "2022-08-04"
    - "2022-11-15"          -> "2022-11-15" (already correct)
    - "20 Nov 2022"         -> "2022-11-20"
    - "29-03-2022"          -> "2022-03-29"
    - "2022-07-03T00:00:00" -> "2022-07-03"
    - "2022/08/14"          -> "2022-08-14"
    - "April 25, 2024"      -> "2024-04-25"

    What is ISO format? YYYY-MM-DD is the international standard
    for dates. Using it everywhere means:
    - Sorting works correctly (alphabetical = chronological)
    - No ambiguity between DD/MM/YYYY and MM/DD/YYYY
    - Every database and tool understands it

    Args:
        date_str: Date string in any format

    Returns:
        Date string in YYYY-MM-DD format, or None if unparseable
    """
    if date_str is None or date_str == "" or pd.isna(date_str):
        return None

    # Already in correct format
    if re.match(r"^\d{4}-\d{2}-\d{2}$", str(date_str)):
        return str(date_str)

    # Has timestamp suffix like "2022-07-03T00:00:00"
    if "T" in str(date_str):
        return str(date_str).split("T")[0]

    try:
        # dateutil_parser handles almost any human-readable date
        # dayfirst=False means MM/DD/YYYY takes priority over DD/MM/YYYY
        parsed = dateutil_parser.parse(
            str(date_str), dayfirst=False
        )
        return parsed.strftime("%Y-%m-%d")
    except Exception:
        return None


def normalize_date_column(
    df: pd.DataFrame,
    col: str
) -> pd.DataFrame:
    """
    Applies normalize_date to an entire DataFrame column.

    Args:
        df: The DataFrame
        col: Column name containing dates

    Returns:
        DataFrame with that column standardized
    """
    if col not in df.columns:
        return df

    df = df.copy()
    df[col] = df[col].apply(normalize_date)
    return df