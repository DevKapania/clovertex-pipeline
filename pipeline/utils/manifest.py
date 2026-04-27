import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
import pandas as pd


def compute_sha256(file_path: str) -> str:
    """
    Computes the SHA-256 checksum of a file.
    
    What is a checksum? Like a fingerprint for a file.
    Two identical files will ALWAYS produce the same checksum.
    If even one character changes, the checksum changes completely.
    
    This lets us verify data hasn't been corrupted or tampered with.
    
    Args:
        file_path: Path to the file
    
    Returns:
        64-character hex string (the SHA-256 hash)
    
    Example:
        "a3f5d8e2b1c4..." (64 characters)
    """
    # sha256() creates a new SHA-256 calculator
    sha256_hash = hashlib.sha256()
    
    # Read file in chunks to handle large files without
    # loading entire file into memory at once
    # 8192 bytes = 8KB chunks
    with open(file_path, "rb") as f:  # "rb" = read binary
        for chunk in iter(lambda: f.read(8192), b""):
            sha256_hash.update(chunk)
    
    # hexdigest() returns the hash as a readable hex string
    return sha256_hash.hexdigest()


def get_parquet_schema(file_path: str) -> Dict[str, str]:
    """
    Reads a Parquet file and returns its column names and data types.
    
    What is a schema? A description of what columns exist and
    what type of data each column holds (text, number, date, etc.)
    
    Args:
        file_path: Path to the Parquet file
    
    Returns:
        Dictionary like {"patient_id": "string", "age": "int64"}
    """
    try:
        df = pd.read_parquet(file_path)
        # Convert pandas dtype objects to strings
        return {col: str(dtype) for col, dtype in df.dtypes.items()}
    except Exception as e:
        return {"error": str(e)}


def get_row_count(file_path: str) -> int:
    """
    Returns the number of rows in a Parquet file.
    
    Args:
        file_path: Path to the Parquet file
    
    Returns:
        Integer row count
    """
    try:
        df = pd.read_parquet(file_path)
        return len(df)
    except Exception:
        return -1


def generate_manifest(
    zone_path: str,
    zone_name: str
) -> Dict[str, Any]:
    """
    Generates a manifest.json for a data lake zone.
    
    A manifest is like a table of contents + quality certificate.
    It tells anyone who opens this zone:
    - What files are here
    - How many rows each file has
    - What columns/schema each file has
    - When it was processed
    - A SHA-256 checksum to verify file integrity
    
    Args:
        zone_path: Path to the zone folder (e.g. "datalake/raw/")
        zone_name: Human readable name (e.g. "raw", "refined")
    
    Returns:
        Dictionary containing the full manifest
    """
    manifest = {
        "zone": zone_name,
        "generated_at": datetime.now(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        ),
        "files": []
    }
    
    # Walk through all files in the zone folder
    zone_path_obj = Path(zone_path)
    
    # Find all parquet files recursively
    parquet_files = list(zone_path_obj.rglob("*.parquet"))
    
    for file_path in sorted(parquet_files):
        file_info = {
            # Just the filename, not full path
            "file_name": file_path.name,
            # Path relative to zone root
            "relative_path": str(
                file_path.relative_to(zone_path_obj)
            ),
            # File size in bytes
            "size_bytes": os.path.getsize(file_path),
            # Number of data rows
            "row_count": get_row_count(str(file_path)),
            # Column names and types
            "schema": get_parquet_schema(str(file_path)),
            # Processing timestamp (ISO 8601)
            "processing_timestamp": datetime.now(
                timezone.utc
            ).strftime("%Y-%m-%dT%H:%M:%SZ"),
            # SHA-256 fingerprint
            "sha256_checksum": compute_sha256(str(file_path))
        }
        manifest["files"].append(file_info)
    
    # Also include non-parquet files (JSON, etc.)
    other_files = [
        f for f in zone_path_obj.rglob("*")
        if f.is_file()
        and f.suffix != ".parquet"
        and f.name != "manifest.json"
    ]
    
    for file_path in sorted(other_files):
        file_info = {
            "file_name": file_path.name,
            "relative_path": str(
                file_path.relative_to(zone_path_obj)
            ),
            "size_bytes": os.path.getsize(file_path),
            "sha256_checksum": compute_sha256(str(file_path))
        }
        manifest["files"].append(file_info)
    
    return manifest


def save_manifest(zone_path: str, zone_name: str) -> str:
    """
    Generates and saves manifest.json to a zone folder.
    
    Args:
        zone_path: Path to the zone folder
        zone_name: Name of the zone
    
    Returns:
        Path where manifest was saved
    """
    manifest = generate_manifest(zone_path, zone_name)
    
    # Save as manifest.json inside the zone folder
    manifest_path = os.path.join(zone_path, "manifest.json")
    
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)
    
    print(f"Manifest saved to: {manifest_path}")
    return manifest_path
