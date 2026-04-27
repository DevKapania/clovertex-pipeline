import pandas as pd
import numpy as np
from typing import Tuple, Dict, Any
from pipeline.utils.logger import log_dataset_stats, log_error


def standardize_alpha(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    """
    Standardizes Site Alpha patient data.

    Alpha CSV already has clean flat structure but needs:
    - Date format standardization (MM/DD/YYYY -> YYYY-MM-DD)
    - Sex column standardized (F/M -> female/male)
    - Column name verification

    Alpha columns:
    patient_id, first_name, last_name, date_of_birth, sex,
    blood_group, admission_dt, discharge_dt,
    contact_phone, contact_email, site
    """
    issues = {
        "dates_standardized": 0,
        "sex_standardized": 0,
        "nulls_found": 0
    }

    df = df.copy()

    # --- Standardize date columns ---
    # Alpha uses MM/DD/YYYY format e.g. "07/15/1945"
    date_cols = ["date_of_birth", "admission_dt", "discharge_dt"]
    for col in date_cols:
        if col in df.columns:
            original = df[col].copy()
            df[col] = pd.to_datetime(
                df[col], errors="coerce"
            ).dt.strftime("%Y-%m-%d")
            changed = (df[col] != original).sum()
            issues["dates_standardized"] += int(changed)

    # --- Standardize sex column ---
    # Alpha uses "F"/"M" -> we want "female"/"male"
    if "sex" in df.columns:
        sex_map = {
            "F": "female", "M": "male",
            "f": "female", "m": "male"
        }
        before = df["sex"].copy()
        df["sex"] = df["sex"].map(sex_map).fillna(df["sex"])
        issues["sex_standardized"] = int(
            (df["sex"] != before).sum()
        )

    # --- Add site column if missing ---
    if "site" not in df.columns:
        df["site"] = "Alpha General Hospital"

    # --- Count nulls ---
    issues["nulls_found"] = int(df.isnull().sum().sum())

    return df, issues


def standardize_beta(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
    """
    Standardizes Site Beta patient data.

    Beta JSON was nested and is now flattened with columns:
    patientID, name_given, name_family, birthDate, gender,
    bloodType, encounter_admissionDate, encounter_dischargeDate,
    encounter_facility, contact_phone, contact_email

    We need to rename ALL columns to match Alpha's schema.
    """
    issues = {
        "columns_renamed": 0,
        "dates_standardized": 0,
        "nulls_found": 0
    }

    df = df.copy()

    # --- Rename columns to match Alpha schema ---
    # Beta name -> Alpha name
    rename_map = {
        "patientID":                "patient_id",
        "name_given":               "first_name",
        "name_family":              "last_name",
        "birthDate":                "date_of_birth",
        "gender":                   "sex",
        "bloodType":                "blood_group",
        "encounter_admissionDate":  "admission_dt",
        "encounter_dischargeDate":  "discharge_dt",
        "encounter_facility":       "site",
        "contact_phone":            "contact_phone",
        "contact_email":            "contact_email"
    }

    df = df.rename(columns=rename_map)
    issues["columns_renamed"] = len(rename_map)

    # --- Standardize date columns ---
    # Beta uses mixed formats e.g. "29-05-2023", "2023-06-03"
    date_cols = ["date_of_birth", "admission_dt", "discharge_dt"]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(
                df[col], errors="coerce", dayfirst=True
            ).dt.strftime("%Y-%m-%d")
            issues["dates_standardized"] += 1

    # --- Count nulls ---
    issues["nulls_found"] = int(df.isnull().sum().sum())

    return df, issues


def unify_patients(
    alpha_df: pd.DataFrame,
    beta_df: pd.DataFrame
) -> Tuple[pd.DataFrame, Dict]:
    """
    Combines Alpha and Beta patient tables into ONE master table.

    This is called a UNION operation — stacking two tables
    that have the same columns on top of each other.

    Like combining two Excel sheets into one.

    Final unified columns:
    patient_id, first_name, last_name, date_of_birth, sex,
    blood_group, admission_dt, discharge_dt,
    contact_phone, contact_email, site

    Args:
        alpha_df: Cleaned Alpha patients DataFrame
        beta_df: Cleaned Beta patients DataFrame

    Returns:
        Tuple of (unified DataFrame, issues dict)
    """
    issues = {
        "alpha_rows": len(alpha_df),
        "beta_rows": len(beta_df),
        "duplicates_removed": 0,
        "total_before_dedup": 0,
        "total_after_dedup": 0
    }

    # Define the exact columns we want in the final table
    target_cols = [
        "patient_id", "first_name", "last_name",
        "date_of_birth", "sex", "blood_group",
        "admission_dt", "discharge_dt",
        "contact_phone", "contact_email", "site"
    ]

    # Keep only target columns from each DataFrame
    # (in case extra columns exist)
    alpha_clean = alpha_df[[
        c for c in target_cols if c in alpha_df.columns
    ]].copy()

    beta_clean = beta_df[[
        c for c in target_cols if c in beta_df.columns
    ]].copy()

    # Stack them together (union)
    # ignore_index=True resets row numbers from 0
    unified = pd.concat(
        [alpha_clean, beta_clean],
        ignore_index=True
    )

    issues["total_before_dedup"] = len(unified)

    # Remove duplicate patient_ids (keep first occurrence)
    before = len(unified)
    unified = unified.drop_duplicates(
        subset=["patient_id"], keep="first"
    )
    issues["duplicates_removed"] = before - len(unified)
    issues["total_after_dedup"] = len(unified)

    # Sort by patient_id for consistency
    unified = unified.sort_values("patient_id").reset_index(
        drop=True
    )

    log_dataset_stats(
        dataset="unified_patients",
        rows_in=issues["total_before_dedup"],
        rows_out=len(unified),
        issues_found=issues
    )

    return unified, issues
