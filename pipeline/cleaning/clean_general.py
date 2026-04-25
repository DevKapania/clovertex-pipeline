import pandas as pd
import numpy as np
from typing import Tuple, Dict, Any
from pipeline.cleaning.clean_dates import normalize_date_column
from pipeline.utils.logger import log_dataset_stats


def clean_medications(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    """
    Cleans the medications log.

    Issues found in medications_log.json:
    1. Dates in 10+ different formats
    2. Empty dosage strings "" instead of NULL
    3. Dosage values with inconsistent casing e.g. "1000MG" vs "1000mg"

    Args:
        df: Raw medications DataFrame

    Returns:
        Tuple of (cleaned DataFrame, issues dict)
    """
    issues = {
        "empty_dosage_fixed": 0,
        "dates_normalized": 0,
        "dosage_casing_fixed": 0,
        "nulls_before": 0,
        "nulls_after": 0
    }

    df = df.copy()
    issues["nulls_before"] = int(df.isnull().sum().sum())

    # --- Fix empty dosage strings ---
    # "" is not the same as NaN/None in pandas
    # We convert empty strings to NaN for consistency
    if "dosage" in df.columns:
        empty_mask = df["dosage"].str.strip() == ""
        issues["empty_dosage_fixed"] = int(empty_mask.sum())
        df.loc[empty_mask, "dosage"] = np.nan

        # Standardize dosage casing: "1000MG" -> "1000mg"
        df["dosage"] = df["dosage"].str.lower().str.strip()
        issues["dosage_casing_fixed"] = int(
            df["dosage"].notna().sum()
        )

    # --- Normalize all date columns ---
    for col in ["start_date", "end_date"]:
        if col in df.columns:
            before = df[col].copy()
            df = normalize_date_column(df, col)
            changed = (df[col] != before).sum()
            issues["dates_normalized"] += int(changed)

    issues["nulls_after"] = int(df.isnull().sum().sum())

    log_dataset_stats(
        dataset="medications_log",
        rows_in=len(df),
        rows_out=len(df),
        issues_found=issues
    )

    return df, issues


def clean_lab_results(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    """
    Cleans site_gamma_lab_results.

    Issues:
    1. Column named 'patient_ref' not 'patient_id'
    2. Dates need standardization
    3. Negative test values are impossible for most lab tests

    Args:
        df: Raw lab results DataFrame

    Returns:
        Tuple of (cleaned DataFrame, issues dict)
    """
    issues = {
        "patient_ref_renamed": False,
        "negative_values_found": 0,
        "dates_normalized": 0,
        "nulls_found": 0
    }

    df = df.copy()

    # --- Rename patient_ref to patient_id ---
    if "patient_ref" in df.columns:
        df = df.rename(columns={"patient_ref": "patient_id"})
        issues["patient_ref_renamed"] = True

    # --- Standardize collection_date ---
    if "collection_date" in df.columns:
        df = normalize_date_column(df, "collection_date")
        issues["dates_normalized"] = 1

    # --- Flag negative test values ---
    # Lab values like ALT, AST, BUN cannot be negative
    if "test_value" in df.columns:
        neg_mask = df["test_value"] < 0
        issues["negative_values_found"] = int(neg_mask.sum())
        # Replace negatives with NaN (mark as unreliable)
        df.loc[neg_mask, "test_value"] = np.nan

    issues["nulls_found"] = int(df.isnull().sum().sum())

    log_dataset_stats(
        dataset="site_gamma_lab_results",
        rows_in=len(df),
        rows_out=len(df),
        issues_found=issues
    )

    return df, issues


def clean_genomics(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    """
    Cleans genomics_variants.

    Issues:
    1. Column named 'patient_ref' not 'patient_id'
    2. allele_frequency must be between 0 and 1
    3. read_depth must be positive
    4. Filter unreliable variants (low read depth)

    What is allele_frequency?
    The proportion of a specific gene variant in a sample.
    Must always be between 0.0 and 1.0 (0% to 100%).

    What is read_depth?
    How many times a DNA position was sequenced.
    Low read_depth = unreliable result. We filter < 10.

    Args:
        df: Raw genomics DataFrame

    Returns:
        Tuple of (cleaned DataFrame, issues dict)
    """
    issues = {
        "patient_ref_renamed": False,
        "invalid_allele_freq": 0,
        "low_read_depth_filtered": 0,
        "dates_normalized": 0,
        "rows_before_filter": len(df),
        "rows_after_filter": 0
    }

    df = df.copy()

    # --- Rename patient_ref to patient_id ---
    if "patient_ref" in df.columns:
        df = df.rename(columns={"patient_ref": "patient_id"})
        issues["patient_ref_renamed"] = True

    # --- Validate allele_frequency (must be 0-1) ---
    if "allele_frequency" in df.columns:
        invalid_mask = (
            (df["allele_frequency"] < 0) |
            (df["allele_frequency"] > 1)
        )
        issues["invalid_allele_freq"] = int(invalid_mask.sum())
        df.loc[invalid_mask, "allele_frequency"] = np.nan

    # --- Filter low read depth variants ---
    # read_depth < 10 means the sequencing result is unreliable
    # This is a standard bioinformatics quality filter
    if "read_depth" in df.columns:
        low_depth_mask = df["read_depth"] < 10
        issues["low_read_depth_filtered"] = int(
            low_depth_mask.sum()
        )
        df = df[~low_depth_mask].reset_index(drop=True)

    # --- Normalize sample_date ---
    if "sample_date" in df.columns:
        df = normalize_date_column(df, "sample_date")
        issues["dates_normalized"] = 1

    issues["rows_after_filter"] = len(df)

    log_dataset_stats(
        dataset="genomics_variants",
        rows_in=issues["rows_before_filter"],
        rows_out=len(df),
        issues_found=issues
    )

    return df, issues


def clean_diagnoses(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    """
    Cleans diagnoses_icd10.

    Issues:
    1. Dates need standardization
    2. Empty notes column (float NaN)
    3. Severity and status need consistent casing

    Args:
        df: Raw diagnoses DataFrame

    Returns:
        Tuple of (cleaned DataFrame, issues dict)
    """
    issues = {
        "dates_normalized": 0,
        "casing_fixed": 0,
        "nulls_found": 0
    }

    df = df.copy()

    # --- Normalize diagnosis_date ---
    if "diagnosis_date" in df.columns:
        df = normalize_date_column(df, "diagnosis_date")
        issues["dates_normalized"] = 1

    # --- Standardize casing for severity and status ---
    for col in ["severity", "status"]:
        if col in df.columns:
            df[col] = df[col].str.lower().str.strip()
            issues["casing_fixed"] += 1

    # --- Standardize is_primary to boolean-like ---
    if "is_primary" in df.columns:
        df["is_primary"] = df["is_primary"].map(
            {"Y": True, "N": False, "y": True, "n": False}
        ).fillna(False)

    issues["nulls_found"] = int(df.isnull().sum().sum())

    log_dataset_stats(
        dataset="diagnoses_icd10",
        rows_in=len(df),
        rows_out=len(df),
        issues_found=issues
    )

    return df, issues


def clean_clinical_notes(
    df: pd.DataFrame
) -> Tuple[pd.DataFrame, Dict]:
    """
    Cleans clinical_notes_metadata.

    Issues:
    1. Dates need standardization
    2. is_addendum needs boolean standardization

    Args:
        df: Raw clinical notes DataFrame

    Returns:
        Tuple of (cleaned DataFrame, issues dict)
    """
    issues = {
        "dates_normalized": 0,
        "addendum_standardized": 0,
        "nulls_found": 0
    }

    df = df.copy()

    # --- Normalize note_date ---
    if "note_date" in df.columns:
        df = normalize_date_column(df, "note_date")
        issues["dates_normalized"] = 1

    # --- Standardize is_addendum ---
    if "is_addendum" in df.columns:
        df["is_addendum"] = df["is_addendum"].map(
            {"Y": True, "N": False, "y": True, "n": False}
        ).fillna(False)
        issues["addendum_standardized"] = 1

    issues["nulls_found"] = int(df.isnull().sum().sum())

    log_dataset_stats(
        dataset="clinical_notes_metadata",
        rows_in=len(df),
        rows_out=len(df),
        issues_found=issues
    )

    return df, issues