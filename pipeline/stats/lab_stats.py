import pandas as pd
import numpy as np
import json
import os
from typing import Dict, Any
from pipeline.utils.logger import log_dataset_stats


def load_reference_ranges(
    ref_path: str = "data/reference/lab_test_ranges.json"
) -> Dict:
    """
    Loads the lab test reference ranges from JSON file.

    What are reference ranges?
    Every lab test has a "normal" range. For example:
    - ALT (liver enzyme): normal is 7-56 U/L
    - If a patient's ALT is 500 U/L → flagged as HIGH

    These ranges come from medical standards and are
    stored in lab_test_ranges.json.

    Returns:
        Dictionary like:
        {"alt": {"min": 7, "max": 56, "unit": "U/L"}, ...}
    """
    try:
        with open(ref_path, "r") as f:
            return json.load(f)
    except Exception:
        return {}


def compute_lab_stats(
    lab_df: pd.DataFrame,
    ref_path: str = "data/reference/lab_test_ranges.json"
) -> pd.DataFrame:
    """
    Computes statistics for each lab test type and flags
    values outside reference ranges.

    What does this produce per test type?
    - count: how many results exist
    - mean, median, std, min, max
    - pct_below_range: % of results below normal
    - pct_above_range: % of results above normal
    - pct_in_range: % of results in normal range

    Why flag out-of-range values?
    Abnormal lab values are clinical alerts.
    High ALT = possible liver disease.
    Low hemoglobin = possible anemia.
    This is the core of clinical decision support.

    Args:
        lab_df: Cleaned lab results DataFrame
        ref_path: Path to reference ranges JSON

    Returns:
        Stats DataFrame with one row per test type
    """
    ref_ranges = load_reference_ranges(ref_path)
    results = []

    # Group by test_name and compute stats
    for test_name, group in lab_df.groupby("test_name"):
        values = group["test_value"].dropna()

        if len(values) == 0:
            continue

        row = {
            "test_name": test_name,
            "count": int(len(values)),
            "mean": round(float(values.mean()), 3),
            "median": round(float(values.median()), 3),
            "std": round(float(values.std()), 3),
            "min": round(float(values.min()), 3),
            "max": round(float(values.max()), 3),
            "unit": group["test_unit"].mode()[0]
                if len(group["test_unit"].dropna()) > 0
                else "unknown",
            "ref_min": None,
            "ref_max": None,
            "pct_below_range": None,
            "pct_above_range": None,
            "pct_in_range": None
        }

        # Look up reference range for this test
        test_key = test_name.lower().strip()
        if test_key in ref_ranges:
            ref = ref_ranges[test_key]
            ref_min = ref.get("min")
            ref_max = ref.get("max")

            row["ref_min"] = ref_min
            row["ref_max"] = ref_max

            if ref_min is not None and ref_max is not None:
                below = (values < ref_min).sum()
                above = (values > ref_max).sum()
                in_range = (
                    (values >= ref_min) &
                    (values <= ref_max)
                ).sum()
                total = len(values)

                row["pct_below_range"] = round(
                    below / total * 100, 2
                )
                row["pct_above_range"] = round(
                    above / total * 100, 2
                )
                row["pct_in_range"] = round(
                    in_range / total * 100, 2
                )

        results.append(row)

    stats_df = pd.DataFrame(results)

    log_dataset_stats(
        dataset="lab_stats",
        rows_in=len(lab_df),
        rows_out=len(stats_df),
        issues_found={"test_types_found": len(results)}
    )

    return stats_df
