import pandas as pd
import numpy as np
from datetime import date
from typing import Dict, Any
from pipeline.utils.logger import log_dataset_stats


def compute_age(date_of_birth: str) -> float:
    """
    Computes a patient's age in years from date of birth.

    What is this calculation?
    We subtract birth year from today's year, then adjust
    if the birthday hasn't happened yet this year.

    Args:
        date_of_birth: Date string in YYYY-MM-DD format

    Returns:
        Age in years as a float, or NaN if unparseable
    """
    try:
        dob = pd.to_datetime(date_of_birth)
        today = pd.Timestamp.today()
        age = (today - dob).days / 365.25
        return round(age, 1)
    except Exception:
        return np.nan


def compute_demographics(
    patients_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Computes a full demographics summary from the unified
    patients table.

    What analytics does this produce?
    1. Age statistics (mean, median, min, max, std)
    2. Age group distribution (0-17, 18-35, 36-60, 60+)
    3. Sex distribution (count + percentage)
    4. Blood group distribution
    5. Site distribution (Alpha vs Beta)

    Why is this useful?
    Demographics tell us if our patient population is
    representative. Regulators and researchers need this
    to understand study bias.

    Args:
        patients_df: Unified patients DataFrame

    Returns:
        Summary DataFrame saved to consumption zone
    """
    results = []

    df = patients_df.copy()

    # --- Compute age for every patient ---
    df["age"] = df["date_of_birth"].apply(compute_age)

    # --- Age Statistics ---
    age_stats = {
        "metric_type": "age_statistics",
        "mean_age": round(df["age"].mean(), 2),
        "median_age": round(df["age"].median(), 2),
        "min_age": round(df["age"].min(), 2),
        "max_age": round(df["age"].max(), 2),
        "std_age": round(df["age"].std(), 2),
        "null_ages": int(df["age"].isna().sum())
    }
    results.append(age_stats)

    # --- Age Group Distribution ---
    # Cut age into bins with labels
    bins = [0, 18, 35, 60, 120]
    labels = ["0-17", "18-35", "36-60", "60+"]
    df["age_group"] = pd.cut(
        df["age"], bins=bins, labels=labels, right=False
    )

    for group, count in df["age_group"].value_counts().items():
        results.append({
            "metric_type": "age_group_distribution",
            "age_group": str(group),
            "count": int(count),
            "percentage": round(
                count / len(df) * 100, 2
            )
        })

    # --- Sex Distribution ---
    for sex, count in df["sex"].value_counts().items():
        results.append({
            "metric_type": "sex_distribution",
            "sex": str(sex),
            "count": int(count),
            "percentage": round(
                count / len(df) * 100, 2
            )
        })

    # --- Blood Group Distribution ---
    for bg, count in df["blood_group"].value_counts().items():
        results.append({
            "metric_type": "blood_group_distribution",
            "blood_group": str(bg),
            "count": int(count),
            "percentage": round(
                count / len(df) * 100, 2
            )
        })

    # --- Site Distribution ---
    for site, count in df["site"].value_counts().items():
        results.append({
            "metric_type": "site_distribution",
            "site": str(site),
            "count": int(count),
            "percentage": round(
                count / len(df) * 100, 2
            )
        })

    summary_df = pd.DataFrame(results)

    log_dataset_stats(
        dataset="demographics_summary",
        rows_in=len(patients_df),
        rows_out=len(summary_df),
        issues_found={"metrics_computed": len(results)}
    )

    return summary_df
