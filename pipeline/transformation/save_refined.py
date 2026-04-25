import os
import pandas as pd
from pipeline.utils.manifest import save_manifest
from pipeline.utils.logger import log_dataset_stats


REFINED_PATH = "datalake/refined"


def save_to_refined(
    df: pd.DataFrame,
    dataset_name: str,
    partition_col: str = None
) -> str:
    """
    Saves a cleaned DataFrame to the refined zone as Parquet.

    What is partitioning?
    Instead of one giant file, we split data into subfolders
    based on a column value. Like organizing files by year.

    Example WITHOUT partition:
    datalake/refined/lab_results.parquet

    Example WITH partition on 'site_name':
    datalake/refined/lab_results/site_name=Alpha/data.parquet
    datalake/refined/lab_results/site_name=Beta/data.parquet
    datalake/refined/lab_results/site_name=Gamma/data.parquet

    Why partition?
    - Faster queries (only read relevant partition)
    - Industry standard for data lakes
    - Required by the assignment for lab results

    Args:
        df: Cleaned DataFrame to save
        dataset_name: Name for the output file/folder
        partition_col: Optional column to partition by

    Returns:
        Path where data was saved
    """
    os.makedirs(REFINED_PATH, exist_ok=True)

    if partition_col and partition_col in df.columns:
        # Save with partitioning
        output_path = os.path.join(REFINED_PATH, dataset_name)
        os.makedirs(output_path, exist_ok=True)

        # Write each partition separately
        for value in df[partition_col].unique():
            partition_df = df[df[partition_col] == value]

            # Clean partition value for folder name
            safe_value = str(value).replace(
                " ", "_"
            ).replace("/", "_")
            partition_folder = os.path.join(
                output_path,
                f"{partition_col}={safe_value}"
            )
            os.makedirs(partition_folder, exist_ok=True)

            partition_path = os.path.join(
                partition_folder, "data.parquet"
            )
            partition_df.to_parquet(
                partition_path, index=False, engine="pyarrow"
            )

        print(f"Saved partitioned: {output_path}")
        return output_path

    else:
        # Save as single Parquet file
        output_path = os.path.join(
            REFINED_PATH, f"{dataset_name}.parquet"
        )
        df.to_parquet(output_path, index=False, engine="pyarrow")
        print(f"Saved: {output_path}")
        return output_path


def save_all_refined(datasets: dict) -> None:
    """
    Saves all cleaned datasets to refined zone and
    generates the manifest.

    Args:
        datasets: Dictionary of {name: DataFrame}
    """
    for name, df in datasets.items():
        # Lab results get partitioned by site
        if name == "lab_results":
            save_to_refined(df, name, partition_col="site_name")
        else:
            save_to_refined(df, name)

    # Generate manifest for refined zone
    save_manifest(REFINED_PATH, "refined")
    print("Refined zone manifest generated!")
