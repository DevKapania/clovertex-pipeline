import pandas as pd
import os
from typing import Tuple, Dict, Any
from pipeline.utils.logger import log_dataset_stats, log_error


def load_csv(file_path: str) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Safely loads a CSV file into a pandas DataFrame.
    
    What is a DataFrame? Think of it like a table in Excel.
    It has rows and columns, and you can filter, sort, and
    calculate on it using Python code.
    
    What problems can CSV files have?
    - Wrong encoding (special characters like é, ü, ñ break things)
    - Inconsistent delimiters (comma vs semicolon vs tab)
    - Empty files
    - Extra whitespace in column names
    
    We handle ALL of these automatically here.
    
    Args:
        file_path: Path to the CSV file
    
    Returns:
        Tuple of (DataFrame, issues_dict)
        - DataFrame: the loaded data
        - issues_dict: problems we found and fixed
    """
    issues = {
        "encoding_fixed": False,
        "whitespace_stripped": False,
        "empty_columns_removed": 0,
        "rows_in": 0,
        "encoding_used": "utf-8"
    }
    
    # Get dataset name from filename for logging
    dataset_name = os.path.basename(file_path).replace(".csv", "")
    
    # --- Step 1: Try reading with UTF-8 encoding first ---
    # UTF-8 is the most common encoding (handles English + most languages)
    try:
        df = pd.read_csv(file_path, encoding="utf-8")
        issues["encoding_used"] = "utf-8"
        
    except UnicodeDecodeError:
        # UTF-8 failed — try latin-1 (handles Western European characters)
        # latin-1 never fails because it accepts any byte value
        try:
            df = pd.read_csv(file_path, encoding="latin-1")
            issues["encoding_fixed"] = True
            issues["encoding_used"] = "latin-1"
            
        except Exception as e:
            log_error(dataset_name, f"Failed to read CSV: {str(e)}")
            # Return empty DataFrame with error info
            return pd.DataFrame(), {"error": str(e)}
    
    except Exception as e:
        log_error(dataset_name, f"Failed to read CSV: {str(e)}")
        return pd.DataFrame(), {"error": str(e)}
    
    # Record how many rows came in
    issues["rows_in"] = len(df)
    rows_in = len(df)
    
    # --- Step 2: Clean column names ---
    # Remove leading/trailing spaces from column names
    # e.g. " patient_id " becomes "patient_id"
    original_cols = list(df.columns)
    df.columns = df.columns.str.strip()
    
    if list(df.columns) != original_cols:
        issues["whitespace_stripped"] = True
    
    # Also strip whitespace from string values in all columns
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].str.strip()
    
    # --- Step 3: Remove completely empty columns ---
    # A column where EVERY row is empty is useless
    before_cols = len(df.columns)
    df = df.dropna(axis=1, how="all")
    issues["empty_columns_removed"] = before_cols - len(df.columns)
    
    # --- Step 4: Log what we found ---
    log_dataset_stats(
        dataset=dataset_name,
        rows_in=rows_in,
        rows_out=len(df),
        issues_found=issues
    )
    
    return df, issues