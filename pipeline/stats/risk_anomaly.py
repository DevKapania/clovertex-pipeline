import pandas as pd
import numpy as np
from pipeline.utils.logger import log_dataset_stats


def compute_variant_hotspots(
    genomics_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Finds the most mutated genes (variant hotspots).

    What is a variant hotspot?
    A gene that appears in many patients' variant data.
    High mutation frequency in a gene like BRCA1 is
    clinically significant — it may indicate cancer risk.

    Method:
    Count unique patients per gene, sort descending.

    Args:
        genomics_df: Cleaned genomics DataFrame

    Returns:
        DataFrame with gene, patient_count, variant_count
    """
    hotspots = (
        genomics_df.groupby("gene")
        .agg(
            variant_count=("variant_id", "count"),
            patient_count=("patient_id", "nunique"),
            pathogenic_count=(
                "clinical_significance",
                lambda x: (
                    x.str.contains(
                        "Pathogenic", na=False
                    )
                ).sum()
            ),
            avg_allele_freq=(
                "allele_frequency", "mean"
            )
        )
        .reset_index()
        .sort_values("patient_count", ascending=False)
    )

    hotspots["avg_allele_freq"] = hotspots[
        "avg_allele_freq"
    ].round(4)

    log_dataset_stats(
        dataset="variant_hotspots",
        rows_in=len(genomics_df),
        rows_out=len(hotspots),
        issues_found={"genes_found": len(hotspots)}
    )

    return hotspots


def compute_high_risk_patients(
    patients_df: pd.DataFrame,
    diagnoses_df: pd.DataFrame,
    genomics_df: pd.DataFrame,
    lab_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Identifies high-risk patients based on multiple criteria.

    A patient is HIGH RISK if they have ANY of:
    1. A Pathogenic or Likely Pathogenic genomic variant
    2. 3 or more diagnoses
    3. A lab value more than 3x above the normal max
       (e.g. ALT > 168 when normal max is 56)
    4. A primary diagnosis with severity = 'severe'

    Why multi-criteria risk scoring?
    No single signal tells the full story.
    A patient with cancer + 5 diagnoses + abnormal labs
    needs immediate attention. This is how clinical
    decision support systems work in real hospitals.

    Args:
        patients_df: Unified patients
        diagnoses_df: Cleaned diagnoses
        genomics_df: Cleaned genomics
        lab_df: Cleaned lab results

    Returns:
        DataFrame of high-risk patients with risk reasons
    """
    risk_flags = {}

    # --- Flag 1: Pathogenic genomic variants ---
    path_mask = genomics_df[
        "clinical_significance"
    ].str.contains("Pathogenic", na=False)

    for pid in genomics_df[path_mask]["patient_id"].unique():
        if pid not in risk_flags:
            risk_flags[pid] = []
        risk_flags[pid].append("pathogenic_variant")

    # --- Flag 2: 3+ diagnoses ---
    diag_counts = (
        diagnoses_df.groupby("patient_id")["diagnosis_id"]
        .count()
    )
    for pid in diag_counts[diag_counts >= 3].index:
        if pid not in risk_flags:
            risk_flags[pid] = []
        risk_flags[pid].append("3_or_more_diagnoses")

    # --- Flag 3: Extreme lab values (3x above max) ---
    # Using ALT and AST as key markers
    extreme_tests = {"alt": 168, "ast": 120, "bun": 60}
    for test, threshold in extreme_tests.items():
        test_df = lab_df[
            lab_df["test_name"].str.lower() == test
        ]
        extreme = test_df[test_df["test_value"] > threshold]
        for pid in extreme["patient_id"].unique():
            if pid not in risk_flags:
                risk_flags[pid] = []
            risk_flags[pid].append(f"extreme_{test}")

    # --- Flag 4: Severe primary diagnosis ---
    severe_primary = diagnoses_df[
        (diagnoses_df["severity"] == "severe") &
        (diagnoses_df["is_primary"] == True)
    ]
    for pid in severe_primary["patient_id"].unique():
        if pid not in risk_flags:
            risk_flags[pid] = []
        risk_flags[pid].append("severe_primary_diagnosis")

    # --- Build output DataFrame ---
    if not risk_flags:
        return pd.DataFrame()

    rows = []
    for pid, flags in risk_flags.items():
        rows.append({
            "patient_id": pid,
            "risk_flag_count": len(flags),
            "risk_reasons": "|".join(sorted(set(flags))),
            "risk_level": (
                "critical" if len(flags) >= 3
                else "high" if len(flags) == 2
                else "moderate"
            )
        })

    high_risk_df = pd.DataFrame(rows).sort_values(
        "risk_flag_count", ascending=False
    ).reset_index(drop=True)

    log_dataset_stats(
        dataset="high_risk_patients",
        rows_in=len(patients_df),
        rows_out=len(high_risk_df),
        issues_found={
            "critical": int(
                (high_risk_df["risk_level"] == "critical").sum()
            ),
            "high": int(
                (high_risk_df["risk_level"] == "high").sum()
            ),
            "moderate": int(
                (high_risk_df["risk_level"] == "moderate").sum()
            )
        }
    )

    return high_risk_df


def detect_anomalies(
    patients_df: pd.DataFrame,
    diagnoses_df: pd.DataFrame,
    lab_df: pd.DataFrame,
    medications_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Detects impossible or highly suspicious clinical scenarios.

    What are clinical anomalies?
    Data combinations that are medically impossible or
    extremely suspicious — signs of data entry errors,
    fraud, or system glitches.

    Anomalies we detect:
    1. Age impossible: patient born in the future
    2. Age too old: patient > 120 years old
    3. Discharge before admission
    4. Lab value incompatible with life
       (e.g. sodium < 100 or > 170 mEq/L is fatal)
    5. Duplicate medication orders
       (same patient, same drug, overlapping dates)

    Args:
        patients_df: Unified patients
        diagnoses_df: Cleaned diagnoses
        lab_df: Cleaned lab results
        medications_df: Cleaned medications

    Returns:
        DataFrame of flagged anomalies
    """
    anomalies = []

    # --- Anomaly 1: Impossible ages ---
    today = pd.Timestamp.today()

    patients_df = patients_df.copy()
    patients_df["dob_parsed"] = pd.to_datetime(
        patients_df["date_of_birth"], errors="coerce"
    )

    # Born in the future
    future_born = patients_df[
        patients_df["dob_parsed"] > today
    ]
    for _, row in future_born.iterrows():
        anomalies.append({
            "patient_id": row["patient_id"],
            "anomaly_type": "future_birth_date",
            "detail": f"DOB: {row['date_of_birth']}",
            "severity": "critical"
        })

    # Age > 120
    patients_df["age"] = (
        today - patients_df["dob_parsed"]
    ).dt.days / 365.25
    too_old = patients_df[patients_df["age"] > 120]
    for _, row in too_old.iterrows():
        anomalies.append({
            "patient_id": row["patient_id"],
            "anomaly_type": "impossible_age",
            "detail": f"Age: {round(row['age'], 1)}",
            "severity": "critical"
        })

    # --- Anomaly 2: Discharge before admission ---
    p = patients_df.copy()
    p["admission_dt"] = pd.to_datetime(
        p["admission_dt"], errors="coerce"
    )
    p["discharge_dt"] = pd.to_datetime(
        p["discharge_dt"], errors="coerce"
    )
    bad_dates = p[
        p["discharge_dt"] < p["admission_dt"]
    ]
    for _, row in bad_dates.iterrows():
        anomalies.append({
            "patient_id": row["patient_id"],
            "anomaly_type": "discharge_before_admission",
            "detail": (
                f"Admit: {row['admission_dt'].date()}, "
                f"Discharge: {row['discharge_dt'].date()}"
            ),
            "severity": "high"
        })

    # --- Anomaly 3: Lab values incompatible with life ---
    incompatible = {
        "sodium":    {"min": 100, "max": 170},
        "potassium": {"min": 1.5, "max": 9.0},
        "glucose":   {"min": 10,  "max": 2000},
        "alt":       {"min": 0,   "max": 5000},
        "ast":       {"min": 0,   "max": 5000},
    }

    for test, limits in incompatible.items():
        test_rows = lab_df[
            lab_df["test_name"].str.lower() == test
        ]
        extreme = test_rows[
            (test_rows["test_value"] < limits["min"]) |
            (test_rows["test_value"] > limits["max"])
        ]
        for _, row in extreme.iterrows():
            anomalies.append({
                "patient_id": row["patient_id"],
                "anomaly_type": "lab_incompatible_with_life",
                "detail": (
                    f"{test}={row['test_value']} "
                    f"(limits: {limits['min']}-{limits['max']})"
                ),
                "severity": "critical"
            })

    # --- Anomaly 4: Duplicate medication orders ---
    if "start_date" in medications_df.columns:
        dups = medications_df[
            medications_df.duplicated(
                subset=[
                    "patient_id",
                    "medication_name",
                    "start_date"
                ],
                keep=False
            )
        ]
        for _, row in dups.iterrows():
            anomalies.append({
                "patient_id": row["patient_id"],
                "anomaly_type": "duplicate_medication_order",
                "detail": (
                    f"{row['medication_name']} "
                    f"on {row['start_date']}"
                ),
                "severity": "moderate"
            })

    if not anomalies:
        anomalies.append({
            "patient_id": "N/A",
            "anomaly_type": "none_found",
            "detail": "No anomalies detected",
            "severity": "none"
        })

    anomaly_df = pd.DataFrame(anomalies)

    log_dataset_stats(
        dataset="anomaly_flags",
        rows_in=len(patients_df),
        rows_out=len(anomaly_df),
        issues_found={
            "critical": int(
                (anomaly_df["severity"] == "critical").sum()
            ),
            "high": int(
                (anomaly_df["severity"] == "high").sum()
            ),
            "moderate": int(
                (anomaly_df["severity"] == "moderate").sum()
            )
        }
    )

    return anomaly_df
