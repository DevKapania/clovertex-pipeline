import os
import pandas as pd
from pipeline.utils.manifest import save_manifest
from pipeline.utils.logger import log_dataset_stats

CONSUMPTION_PATH = "datalake/consumption"


def save_to_consumption(
    df: pd.DataFrame,
    dataset_name: str
) -> str:
    """
    Saves an analytics result to the consumption zone.

    What is the consumption zone?
    The final layer of the data lake.
    Raw zone    = original files (never touched)
    Refined zone = cleaned files
    Consumption zone = analytics-ready outputs

    Think of it like a restaurant:
    Raw zone     = ingredients from supplier
    Refined zone = prepped ingredients in kitchen
    Consumption zone = finished dishes ready to serve

    Args:
        df: Analytics DataFrame to save
        dataset_name: Output filename

    Returns:
        Path where file was saved
    """
    os.makedirs(CONSUMPTION_PATH, exist_ok=True)
    output_path = os.path.join(
        CONSUMPTION_PATH, f"{dataset_name}.parquet"
    )
    df.to_parquet(output_path, index=False, engine="pyarrow")
    print(f"Saved to consumption: {output_path}")
    return output_path


def save_all_consumption(analytics: dict) -> None:
    """
    Saves all analytics results and generates manifest.

    Args:
        analytics: Dictionary of {name: DataFrame}
    """
    for name, df in analytics.items():
        save_to_consumption(df, name)

    # Generate manifest for consumption zone
    save_manifest(CONSUMPTION_PATH, "consumption")
    print("Consumption zone manifest generated!")