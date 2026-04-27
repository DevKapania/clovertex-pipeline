import pandas as pd
import json
import os
from typing import Tuple, Dict, Any
from pipeline.utils.logger import log_dataset_stats, log_error


def flatten_record(record: dict, prefix: str = "") -> dict:
    """
    Flattens a nested dictionary into a single flat dictionary.

    What is flattening? Converting nested objects into flat columns.

    Example BEFORE flattening:
    {
        "patientID": "BETA-001",
        "name": {
            "given": "Betty",
            "family": "Sanchez"
        },
        "encounter": {
            "admissionDate": "2023-05-29",
            "facility": "Beta Medical Center"
        }
    }

    Example AFTER flattening:
    {
        "patientID": "BETA-001",
        "name_given": "Betty",
        "name_family": "Sanchez",
        "encounter_admissionDate": "2023-05-29",
        "encounter_facility": "Beta Medical Center"
    }

    Why do we need this?
    pandas DataFrames are FLAT tables — like Excel.
    They cannot store nested objects inside cells.
    So we must flatten nested JSON before loading into a DataFrame.

    Args:
        record: A single JSON record (dictionary)
        prefix: Used internally for recursion (leave empty when calling)

    Returns:
        Flat dictionary with no nested objects
    """
    flat = {}
    for key, value in record.items():
        # Build the new key name
        new_key = f"{prefix}_{key}" if prefix else key

        if isinstance(value, dict):
            # Value is a nested dict — recurse into it
            nested = flatten_record(value, prefix=new_key)
            flat.update(nested)
        elif isinstance(value, list):
            # Value is a list — convert to string representation
            # (lists inside records are rare in our data)
            flat[new_key] = json.dumps(value)
        else:
            # Value is a simple scalar (string, number, bool, None)
            flat[new_key] = value

    return flat


def load_json(file_path: str) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Safely loads a JSON file into a pandas DataFrame.

    Handles ALL JSON structures found in our clinical data:

    Type 1 - Flat array (medications_log.json):
    [
        {"medication_id": "MED-001", "patient_id": "ALPHA-001"},
        {"medication_id": "MED-002", "patient_id": "BETA-001"}
    ]

    Type 2 - Nested array (site_beta_patients.json):
    [
        {
            "patientID": "BETA-001",
            "name": {"given": "Betty", "family": "Sanchez"},
            "encounter": {"admissionDate": "2023-05-29"}
        }
    ]

    Type 3 - Object with data inside a key:
    {
        "patients": [{"id": "001", ...}]
    }

    Type 4 - JSON Lines (one object per line):
    {"id": "001"}
    {"id": "002"}

    Args:
        file_path: Path to the JSON file

    Returns:
        Tuple of (DataFrame, issues_dict)
    """
    issues = {
        "json_type_detected": "unknown",
        "nested_key_found": None,
        "rows_in": 0,
        "encoding_fixed": False,
        "records_flattened": False,
        "columns_after_flatten": 0
    }

    dataset_name = os.path.basename(file_path).replace(".json", "")

    # --- Step 1: Read the raw JSON file ---
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            raw = json.load(f)

    except UnicodeDecodeError:
        try:
            with open(file_path, "r", encoding="latin-1") as f:
                raw = json.load(f)
            issues["encoding_fixed"] = True

        except Exception as e:
            log_error(dataset_name, f"Failed to read JSON: {str(e)}")
            return pd.DataFrame(), {"error": str(e)}

    except json.JSONDecodeError:
        # Try JSON Lines format
        try:
            df = pd.read_json(file_path, lines=True)
            issues["json_type_detected"] = "jsonlines"
            issues["rows_in"] = len(df)
            log_dataset_stats(
                dataset=dataset_name,
                rows_in=len(df),
                rows_out=len(df),
                issues_found=issues
            )
            return df, issues

        except Exception as e:
            log_error(dataset_name, f"Failed to read JSON Lines: {str(e)}")
            return pd.DataFrame(), {"error": str(e)}

    except Exception as e:
        log_error(dataset_name, f"Failed to read JSON: {str(e)}")
        return pd.DataFrame(), {"error": str(e)}

    # --- Step 2: Normalize structure to a list of records ---
    if isinstance(raw, list):
        records = raw
        issues["json_type_detected"] = "array"

    elif isinstance(raw, dict):
        # Find which key contains the list of records
        list_keys = [k for k, v in raw.items() if isinstance(v, list)]
        if list_keys:
            data_key = list_keys[0]
            records = raw[data_key]
            issues["json_type_detected"] = "nested_object"
            issues["nested_key_found"] = data_key
        else:
            records = [raw]
            issues["json_type_detected"] = "single_object"
    else:
        log_error(dataset_name, "Unknown JSON structure")
        return pd.DataFrame(), {"error": "Unknown JSON structure"}

    # --- Step 3: Check if records have nested objects ---
    # Look at first record to detect nesting
    needs_flattening = False
    if records:
        sample = records[0]
        for value in sample.values():
            if isinstance(value, dict):
                needs_flattening = True
                break

    # --- Step 4: Flatten if needed ---
    if needs_flattening:
        records = [flatten_record(r) for r in records]
        issues["records_flattened"] = True

    # --- Step 5: Convert to DataFrame ---
    df = pd.DataFrame(records)

    # --- Step 6: Clean column names ---
    df.columns = df.columns.str.strip()
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].str.strip()

    issues["rows_in"] = len(df)
    issues["columns_after_flatten"] = len(df.columns)

    log_dataset_stats(
        dataset=dataset_name,
        rows_in=len(df),
        rows_out=len(df),
        issues_found=issues
    )

    return df, issues
