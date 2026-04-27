import json
import sys
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional


def get_timestamp() -> str:
    """
    Returns current UTC time in ISO 8601 format.
    Example: "2024-06-15T10:30:00Z"
    
    What is ISO 8601? A standard way to write dates/times
    so every computer in the world understands it the same way.
    """
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def log_dataset_stats(
    dataset: str,
    rows_in: int,
    rows_out: int,
    issues_found: Optional[Dict[str, Any]] = None,
    extra: Optional[Dict[str, Any]] = None
) -> None:
    """
    Prints a structured JSON log to stdout for each dataset processed.
    
    This is REQUIRED by the assignment. Every time we process a file,
    we must output exactly this format.
    
    Args:
        dataset: Name of the dataset (e.g. "site_alpha_patients")
        rows_in: How many rows we started with
        rows_out: How many rows remain after cleaning
        issues_found: Dictionary of problems we fixed
        extra: Any additional info to include
    
    Example output:
    {
        "dataset": "site_alpha_patients",
        "rows_in": 370,
        "rows_out": 350,
        "issues_found": {
            "duplicates_removed": 20,
            "nulls_handled": 15
        },
        "processing_timestamp": "2024-06-15T10:30:00Z"
    }
    """
    # Build the log entry
    log_entry = {
        "dataset": dataset,
        "rows_in": rows_in,
        "rows_out": rows_out,
        "issues_found": issues_found or {},
        "processing_timestamp": get_timestamp()
    }
    
    # Add any extra fields if provided
    if extra:
        log_entry.update(extra)
    
    # Print to stdout as formatted JSON
    # indent=2 makes it human-readable (pretty printed)
    print(json.dumps(log_entry, indent=2))
    
    # Flush immediately so logs appear in real time
    sys.stdout.flush()


def log_pipeline_start(pipeline_name: str) -> None:
    """Logs when the entire pipeline begins."""
    entry = {
        "event": "pipeline_start",
        "pipeline": pipeline_name,
        "timestamp": get_timestamp()
    }
    print(json.dumps(entry, indent=2))
    sys.stdout.flush()


def log_pipeline_end(pipeline_name: str, success: bool) -> None:
    """Logs when the entire pipeline finishes."""
    entry = {
        "event": "pipeline_end",
        "pipeline": pipeline_name,
        "success": success,
        "timestamp": get_timestamp()
    }
    print(json.dumps(entry, indent=2))
    sys.stdout.flush()


def log_error(dataset: str, error_message: str) -> None:
    """Logs errors in the same JSON format."""
    entry = {
        "event": "error",
        "dataset": dataset,
        "error": error_message,
        "timestamp": get_timestamp()
    }
    # Errors go to stderr (separate stream from normal logs)
    print(json.dumps(entry, indent=2), file=sys.stderr)
    sys.stderr.flush()
