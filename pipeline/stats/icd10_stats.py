import pandas as pd
import json
from typing import Dict
from pipeline.utils.logger import log_dataset_stats


def load_icd10_chapters(
    ref_path: str = "data/reference/icd10_chapters.csv"
) -> pd.DataFrame:
    """
    Loads the ICD-10 chapter reference table.

    What is ICD-10?
    International Classification of Diseases, 10th revision.
    Every disease/condition has a code like:
    - M54.5 = Low back pain
    - J45   = Asthma
    - E11   = Type 2 diabetes

    Codes are grouped into chapters:
    - A00-B99 = Infectious diseases
    - C00-D49 = Neoplasms (cancers)
    - M00-M99 = Musculoskeletal diseases

    Returns:
        DataFrame with code_range and chapter_name columns
    """
    return pd.read_csv(ref_path)


def get_chapter_for_code(
    icd_code: str,
    chapters_df: pd.DataFrame
) -> str:
    """
    Maps a single ICD-10 code to its chapter name.

    How does this work?
    ICD-10 codes start with a letter followed by numbers.
    The letter tells us the broad category.
    We compare against known ranges to find the chapter.

    Args:
        icd_code: An ICD-10 code like "M54.5"
        chapters_df: Reference table of chapters

    Returns:
        Chapter name string
    """
    if not icd_code or pd.isna(icd_code):
        return "Unknown"

    code = str(icd_code).strip().upper()

    for _, row in chapters_df.iterrows():
        code_range = str(row["code_range"])

        # Parse range like "A00-B99"
        try:
            parts = code_range.split("-")
            if len(parts) == 2:
                start = parts[0].strip()
                end = parts[1].strip()

                # Compare first letter
                if len(code) > 0 and len(start) > 0:
                    code_letter = code[0]
                    start_letter = start[0]
                    end_letter = end[0]

                    if start_letter <= code_letter <= end_letter:
                        return str(row["chapter_name"])
        except Exception:
            continue

    return "Other"


def compute_icd10_top15(
    diagnoses_df: pd.DataFrame,
    ref_path: str = "data/reference/icd10_chapters.csv"
) -> pd.DataFrame:
    """
    Computes the top 15 ICD-10 chapters by patient count.

    Why top 15?
    The assignment specifically requires top 15.
    This shows which disease categories affect the most patients.

    Method:
    1. Map each diagnosis code to its chapter
    2. Count unique patients per chapter
    3. Return top 15 sorted by patient count descending

    Args:
        diagnoses_df: Cleaned diagnoses DataFrame
        ref_path: Path to ICD-10 chapters CSV

    Returns:
        DataFrame with top 15 chapters and patient counts
    """
    chapters_df = load_icd10_chapters(ref_path)

    df = diagnoses_df.copy()

    # Map each code to its chapter
    df["chapter"] = df["icd10_code"].apply(
        lambda x: get_chapter_for_code(x, chapters_df)
    )

    # Count unique patients per chapter
    # (one patient with 3 diagnoses in same chapter = 1 count)
    chapter_counts = (
        df.groupby("chapter")["patient_id"]
        .nunique()
        .reset_index()
        .rename(columns={"patient_id": "patient_count"})
        .sort_values("patient_count", ascending=False)
        .head(15)
        .reset_index(drop=True)
    )

    # Add rank column
    chapter_counts["rank"] = range(1, len(chapter_counts) + 1)

    log_dataset_stats(
        dataset="icd10_top15",
        rows_in=len(diagnoses_df),
        rows_out=len(chapter_counts),
        issues_found={
            "chapters_found": len(chapter_counts),
            "total_diagnoses": len(diagnoses_df)
        }
    )

    return chapter_counts