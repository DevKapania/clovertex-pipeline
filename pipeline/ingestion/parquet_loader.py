import pandas as pd
import os
from typing import Tuple, Dict, Any
from pipeline.utils.logger import log_dataset_stats, log_error


def load_parquet(file_path: str) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Safely loads a Parquet file into a pandas DataFrame.
    
    What is Parquet?
    - A binary file format (not human-readable like CSV)
    - Stores data by COLUMN instead of by row
    - Automatically compressed (files are 5-10x smaller than CSV)
    - Much faster to read for analytics
    - Preserves data types (knows if a column is integer vs string)
    
    Pros of Parquet over CSV:
    - Much smaller file size
    - Much faster to read
    - Preserves exact data types
    - Industry standard for data lakes (AWS, Google, Azure all use it)
    
    Cons:
    - Not human readable (can't open in Notepad)
    - Need special libraries to read (pyarrow, pandas)
    
    Args:
        file_path: Path to the Parquet file
    
    Returns:
        Tuple of (DataFrame, issues_dict)
    """
    issues = {
        "rows_in": 0,
        "columns_found": 0,
        "engine_used": "pyarrow"
    }
    
    dataset_name = os.path.basename(file_path).replace(".parquet", "")
    
    # --- Try reading with pyarrow engine first ---
    # pyarrow is faster and more reliable
    try:
        df = pd.read_parquet(file_path, engine="pyarrow")
        issues["engine_used"] = "pyarrow"
        
    except Exception:
        # Fall back to fastparquet if pyarrow fails
        try:
            df = pd.read_parquet(file_path, engine="fastparquet")
            issues["engine_used"] = "fastparquet"
            
        except Exception as e:
            log_error(dataset_name, f"Failed to read Parquet: {str(e)}")
            return pd.DataFrame(), {"error": str(e)}
    
    issues["rows_in"] = len(df)
    issues["columns_found"] = len(df.columns)
    
    # Clean column names
    df.columns = df.columns.str.strip()
    
    log_dataset_stats(
        dataset=dataset_name,
        rows_in=len(df),
        rows_out=len(df),
        issues_found=issues
    )
    
    return df, issues