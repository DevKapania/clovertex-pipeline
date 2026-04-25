import os
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")  # Non-interactive backend for saving files
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import seaborn as sns
from pipeline.utils.logger import log_dataset_stats

# Output folder for all plots
PLOTS_PATH = "datalake/consumption/plots"

# ── Global style ──────────────────────────────────────────
sns.set_theme(style="whitegrid", palette="muted")
COLORS = {
    "blue":   "#4C72B0",
    "green":  "#55A868",
    "red":    "#C44E52",
    "orange": "#DD8452",
    "purple": "#8172B2",
    "teal":   "#64B5CD",
}


def _save(fig: plt.Figure, filename: str) -> str:
    """Saves figure to plots folder and closes it."""
    os.makedirs(PLOTS_PATH, exist_ok=True)
    path = os.path.join(PLOTS_PATH, filename)
    fig.savefig(path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved plot: {path}")
    return path


# ── Plot 1: Patient Demographics ─────────────────────────
def plot_demographics(
    consumption_path: str = "datalake/consumption"
) -> str:
    """
    Plot 1: Age distribution histogram + sex bar chart.

    Two subplots side by side:
    Left  → histogram of patient ages
    Right → bar chart of male vs female count

    What is a histogram?
    A chart that shows how values are distributed.
    X axis = age ranges (bins), Y axis = patient count.
    Taller bars = more patients in that age range.

    What is a subplot?
    Multiple charts inside one figure.
    """
    df = pd.read_parquet(
        os.path.join(consumption_path, "demographics_summary.parquet")
    )
    patients = pd.read_parquet(
        "datalake/refined/unified_patients.parquet"
    )

    # Compute ages
    patients["age"] = (
        pd.Timestamp.today() -
        pd.to_datetime(patients["date_of_birth"], errors="coerce")
    ).dt.days / 365.25

    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    fig.suptitle(
        "Patient Demographics Overview",
        fontsize=16, fontweight="bold", y=1.02
    )

    # ── Left: Age histogram ──
    ax1 = axes[0]
    valid_ages = patients["age"].dropna()
    ax1.hist(
        valid_ages, bins=20,
        color=COLORS["blue"], edgecolor="white", alpha=0.85
    )
    ax1.axvline(
        valid_ages.mean(), color=COLORS["red"],
        linestyle="--", linewidth=2,
        label=f"Mean age: {valid_ages.mean():.1f}"
    )
    ax1.axvline(
        valid_ages.median(), color=COLORS["orange"],
        linestyle="--", linewidth=2,
        label=f"Median age: {valid_ages.median():.1f}"
    )
    ax1.set_title("Age Distribution", fontsize=13)
    ax1.set_xlabel("Age (years)")
    ax1.set_ylabel("Number of Patients")
    ax1.legend()

    # ── Right: Sex bar chart ──
    ax2 = axes[1]
    sex_data = df[df["metric_type"] == "sex_distribution"]
    bars = ax2.bar(
        sex_data["sex"],
        sex_data["count"],
        color=[COLORS["blue"], COLORS["red"], COLORS["teal"]],
        edgecolor="white", alpha=0.85
    )
    # Add count labels on top of bars
    for bar in bars:
        ax2.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 2,
            str(int(bar.get_height())),
            ha="center", va="bottom", fontweight="bold"
        )
    ax2.set_title("Sex Distribution", fontsize=13)
    ax2.set_xlabel("Sex")
    ax2.set_ylabel("Number of Patients")

    plt.tight_layout()
    return _save(fig, "plot1_demographics.png")


# ── Plot 2: Top 15 ICD-10 Chapters ───────────────────────
def plot_icd10_top15(
    consumption_path: str = "datalake/consumption"
) -> str:
    """
    Plot 2: Horizontal bar chart of top 15 ICD-10 chapters.

    Why horizontal bars?
    Chapter names are long text strings.
    Horizontal bars give more space for labels on Y axis.
    Much more readable than vertical bars for text labels.
    """
    df = pd.read_parquet(
        os.path.join(consumption_path, "icd10_top15.parquet")
    )

    df = df.sort_values("patient_count", ascending=True)

    fig, ax = plt.subplots(figsize=(12, 8))

    bars = ax.barh(
        df["chapter"],
        df["patient_count"],
        color=COLORS["teal"],
        edgecolor="white",
        alpha=0.85
    )

    # Add count labels at end of each bar
    for bar, val in zip(bars, df["patient_count"]):
        ax.text(
            bar.get_width() + 0.5,
            bar.get_y() + bar.get_height() / 2,
            str(int(val)),
            va="center", ha="left", fontsize=9
        )

    ax.set_title(
        "Top 15 ICD-10 Disease Chapters by Patient Count",
        fontsize=14, fontweight="bold"
    )
    ax.set_xlabel("Number of Patients")
    ax.set_ylabel("ICD-10 Chapter")
    plt.tight_layout()
    return _save(fig, "plot2_icd10_top15.png")


# ── Plot 3: Lab Results Distribution ─────────────────────
def plot_lab_distributions(
    consumption_path: str = "datalake/consumption"
) -> str:
    """
    Plot 3: Distribution plots for lab tests with
    reference range boundaries overlaid.

    Shows distributions for ALT and AST (liver enzymes)
    with colored zones:
    Green zone = normal range
    Red lines  = reference range boundaries

    What is a KDE plot?
    Kernel Density Estimate — a smooth curve showing
    the distribution of values. Like a smoothed histogram.
    """
    # Read partitioned folder — convert all cols to string safe
    import glob
    parts = glob.glob(
        "datalake/refined/lab_results/**/*.parquet",
        recursive=True
    )
    lab_df = pd.concat(
        [pd.read_parquet(p) for p in sorted(parts)],
        ignore_index=True
    )
    # Fix mixed type columns
    lab_df["site_name"] = lab_df["site_name"].astype(str)
    stats_df = pd.read_parquet(
        os.path.join(consumption_path, "lab_stats.parquet")
    )

    # Pick 2 most common tests that have reference ranges
    test_names = (
        lab_df.groupby("test_name")["lab_result_id"]
        .count()
        .sort_values(ascending=False)
        .head(4)
        .index.tolist()
    )

    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle(
        "Lab Results Distribution with Reference Ranges",
        fontsize=15, fontweight="bold"
    )
    axes = axes.flatten()

    for i, test_name in enumerate(test_names):
        ax = axes[i]
        test_vals = lab_df[
            lab_df["test_name"] == test_name
        ]["test_value"].dropna()

        # KDE plot
        sns.kdeplot(
            test_vals, ax=ax,
            color=COLORS["blue"], fill=True, alpha=0.4
        )

        # Reference range lines from stats
        stats_row = stats_df[
            stats_df["test_name"] == test_name
        ]
        if not stats_row.empty:
            ref_min = stats_row["ref_min"].values[0]
            ref_max = stats_row["ref_max"].values[0]

            if ref_min is not None and not pd.isna(ref_min):
                ax.axvline(
                    ref_min, color=COLORS["green"],
                    linestyle="--", linewidth=2,
                    label=f"Ref min: {ref_min}"
                )
            if ref_max is not None and not pd.isna(ref_max):
                ax.axvline(
                    ref_max, color=COLORS["red"],
                    linestyle="--", linewidth=2,
                    label=f"Ref max: {ref_max}"
                )

        ax.set_title(f"{test_name.upper()} Distribution")
        ax.set_xlabel("Test Value")
        ax.set_ylabel("Density")
        ax.legend(fontsize=8)

    plt.tight_layout()
    return _save(fig, "plot3_lab_distributions.png")


# ── Plot 4: Genomics Scatter ──────────────────────────────
def plot_genomics_scatter(
    consumption_path: str = "datalake/consumption"
) -> str:
    """
    Plot 4: Allele frequency vs read depth scatter plot,
    colored by clinical significance.

    X axis = allele_frequency (0.0 to 1.0)
    Y axis = read_depth (how many times sequenced)
    Color  = clinical significance category

    What does this tell us?
    High read_depth + clear allele_frequency = reliable result
    Low read_depth = unreliable (we already filtered these)
    Pathogenic variants in top-right = high confidence danger
    """
    df = pd.read_parquet(
        "datalake/refined/genomics_variants.parquet"
    )

    # Map significance to colors
    sig_colors = {
        "Pathogenic":            COLORS["red"],
        "Likely Pathogenic":     COLORS["orange"],
        "Uncertain Significance":COLORS["blue"],
        "Likely Benign":         COLORS["teal"],
        "Benign":                COLORS["green"],
    }

    fig, ax = plt.subplots(figsize=(12, 7))

    for sig, color in sig_colors.items():
        subset = df[df["clinical_significance"] == sig]
        if len(subset) > 0:
            ax.scatter(
                subset["allele_frequency"],
                subset["read_depth"],
                c=color, label=sig,
                alpha=0.6, s=30, edgecolors="none"
            )

    ax.set_title(
        "Genomic Variants: Allele Frequency vs Read Depth",
        fontsize=14, fontweight="bold"
    )
    ax.set_xlabel("Allele Frequency")
    ax.set_ylabel("Read Depth")
    ax.legend(title="Clinical Significance", loc="upper right")
    plt.tight_layout()
    return _save(fig, "plot4_genomics_scatter.png")


# ── Plot 5: High Risk Patient Summary ────────────────────
def plot_high_risk_summary(
    consumption_path: str = "datalake/consumption"
) -> str:
    """
    Plot 5: Summary of high-risk patient cohort.

    Two subplots:
    Left  → bar chart of risk levels (critical/high/moderate)
    Right → bar chart of most common risk reasons
    """
    df = pd.read_parquet(
        os.path.join(consumption_path, "high_risk_patients.parquet")
    )

    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    fig.suptitle(
        "High-Risk Patient Cohort Summary",
        fontsize=15, fontweight="bold"
    )

    # ── Left: Risk level distribution ──
    ax1 = axes[0]
    level_counts = df["risk_level"].value_counts()
    level_colors = {
        "critical": COLORS["red"],
        "high":     COLORS["orange"],
        "moderate": COLORS["teal"]
    }
    bar_colors = [
        level_colors.get(l, COLORS["blue"])
        for l in level_counts.index
    ]
    bars = ax1.bar(
        level_counts.index,
        level_counts.values,
        color=bar_colors,
        edgecolor="white"
    )
    for bar in bars:
        ax1.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 0.5,
            str(int(bar.get_height())),
            ha="center", fontweight="bold"
        )
    ax1.set_title("Patients by Risk Level")
    ax1.set_xlabel("Risk Level")
    ax1.set_ylabel("Number of Patients")

    # ── Right: Top risk reasons ──
    ax2 = axes[1]
    all_reasons = []
    for reasons_str in df["risk_reasons"].dropna():
        all_reasons.extend(reasons_str.split("|"))

    reason_counts = (
        pd.Series(all_reasons)
        .value_counts()
        .head(6)
    )
    ax2.barh(
        reason_counts.index,
        reason_counts.values,
        color=COLORS["purple"],
        edgecolor="white",
        alpha=0.85
    )
    ax2.set_title("Most Common Risk Factors")
    ax2.set_xlabel("Number of Patients")
    ax2.set_ylabel("Risk Factor")

    plt.tight_layout()
    return _save(fig, "plot5_high_risk_summary.png")


# ── Plot 6: Data Quality Overview ────────────────────────
def plot_data_quality(
) -> str:
    """
    Plot 6: Pipeline data quality metrics overview.

    Shows a summary of all cleaning decisions made:
    - Duplicates removed
    - Nulls handled
    - Values filtered
    - Dates normalized

    Why is this important?
    Data quality transparency is required in clinical settings.
    Regulators need to know what was changed and why.
    This chart is the visual proof of our cleaning work.
    """
    # Quality metrics from our pipeline (hardcoded from
    # the actual cleaning results we saw in Phase 4)
    metrics = {
        "Duplicates Removed\n(patients)":     30,
        "Negative Lab Values\nFixed":          31,
        "Low Read-Depth\nVariants Filtered":   42,
        "Empty Dosages\nFixed":                35,
        "Dates Normalized\n(medications)":     3026,
        "Diagnoses Notes\nCol Removed":        1,
    }

    fig, ax = plt.subplots(figsize=(12, 6))

    bars = ax.bar(
        list(metrics.keys()),
        list(metrics.values()),
        color=[
            COLORS["red"], COLORS["orange"],
            COLORS["purple"], COLORS["teal"],
            COLORS["blue"], COLORS["green"]
        ],
        edgecolor="white",
        alpha=0.85
    )

    for bar in bars:
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 1,
            str(int(bar.get_height())),
            ha="center", va="bottom",
            fontweight="bold", fontsize=10
        )

    ax.set_title(
        "Data Quality Overview — Pipeline Cleaning Metrics",
        fontsize=14, fontweight="bold"
    )
    ax.set_ylabel("Count of Issues Fixed")
    ax.set_xlabel("Quality Metric")
    plt.xticks(fontsize=9)
    plt.tight_layout()
    return _save(fig, "plot6_data_quality.png")


# ── Run all plots ─────────────────────────────────────────
def generate_all_plots() -> None:
    """Generates all 6 required plots."""
    print("Generating plots...")
    plot_demographics()
    print("Plot 1 done")
    plot_icd10_top15()
    print("Plot 2 done")
    plot_lab_distributions()
    print("Plot 3 done")
    plot_genomics_scatter()
    print("Plot 4 done")
    plot_high_risk_summary()
    print("Plot 5 done")
    plot_data_quality()
    print("Plot 6 done")
    print("All 6 plots saved to datalake/consumption/plots/")