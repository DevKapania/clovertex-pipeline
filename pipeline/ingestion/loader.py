import os
import shutil
import pandas as pd
from typing import Dict, Tuple, Any
from pipeline.ingestion.csv_loader import load_csv
from pipeline.ingestion.json_loader import load_json
from pipeline.ingestion.parquet_loader import load_parquet
from pipeline.utils.logger import log_error


# Map file extensions to loader functions
# This is called a "dispatch table" — a dictionary of functions
LOADERS = {
    ".csv": load_csv,
    ".json": load_json,
    ".parquet": load_parquet,
}


def ingest_file(
    file_path: str,
    raw_zone_path: str = "datalake/raw"
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Master ingestion function. 
    
    Given ANY file path, automatically:
    1. Detects the file format from extension
    2. Calls the right loader
    3. Copies the original file to datalake/raw/ (untouched)
    4. Returns the loaded DataFrame
    
    This is called the "Strategy Pattern" in software engineering —
    we pick the right strategy (loader) based on input type.
    
    Args:
        file_path: Path to any data file (CSV, JSON, or Parquet)
        raw_zone_path: Where to copy original files
    
    Returns:
        Tuple of (DataFrame, issues_dict)
    """
    # --- Step 1: Validate file exists ---
    if not os.path.exists(file_path):
        log_error("unknown", f"File not found: {file_path}")
        return pd.DataFrame(), {"error": "File not found"}
    
    # --- Step 2: Detect format from extension ---
    # os.path.splitext("patients.csv") returns ("patients", ".csv")
    _, extension = os.path.splitext(file_path.lower())
    
    if extension not in LOADERS:
        log_error(
            os.path.basename(file_path),
            f"Unsupported format: {extension}"
        )
        return pd.DataFrame(), {"error": f"Unsupported: {extension}"}
    
    # --- Step 3: Copy original to raw zone (untouched) ---
    # The raw zone is a permanent archive of original files
    os.makedirs(raw_zone_path, exist_ok=True)
    dest_path = os.path.join(raw_zone_path, os.path.basename(file_path))
    
    if not os.path.exists(dest_path):
        shutil.copy2(file_path, dest_path)
    
    # --- Step 4: Load using the right loader ---
    loader_function = LOADERS[extension]
    df, issues = loader_function(file_path)
    
    return df, issues


def ingest_all(
    data_folder: str = "data",
    raw_zone_path: str = "datalake/raw"
) -> Dict[str, pd.DataFrame]:
    """
    Ingests ALL files from the data folder at once.
    
    Walks through every file in data/ folder,
    loads each one, and returns a dictionary where:
    - Key = filename without extension (e.g. "site_alpha_patients")
    - Value = pandas DataFrame with the data
    
    Args:
        data_folder: Folder containing raw data files
        raw_zone_path: Where to copy original files
    
    Returns:
        Dictionary of {dataset_name: DataFrame}
    """
    datasets = {}
    
    # Walk through all files in data folder
    for root, dirs, files in os.walk(data_folder):
        # Skip reference folder for now
        if "reference" in root:
            continue
            
        for filename in sorted(files):
            # Skip hidden files (like .DS_Store on Mac)
            if filename.startswith("."):
                continue
            
            file_path = os.path.join(root, filename)
            _, ext = os.path.splitext(filename.lower())
            
            # Only process supported formats
            if ext not in LOADERS:
                continue
            
            # Dataset name = filename without extension
            dataset_name = filename.replace(ext, "")
            
            df, issues = ingest_file(file_path, raw_zone_path)
            
            if not df.empty:
                datasets[dataset_name] = df
    
    return datasets