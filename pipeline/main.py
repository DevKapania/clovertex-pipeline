"""
Clovertex Clinical Data Pipeline
=================================
Master entry point. Runs all tasks end-to-end:

Task 1: Ingest raw data → datalake/raw/
Task 2: Clean & unify  → datalake/refined/
Task 3: Analytics      → datalake/consumption/
Task 4: Visualizations → datalake/consumption/plots/

Run with:
    python -m pipeline.main
"""

import sys
import os
from pipeline.utils.logger import (
    log_pipeline_start,
    log_pipeline_end,
    log_error
)


def run_pipeline() -> int:
    """
    Runs the full Clovertex pipeline end-to-end.

    Returns:
        0 if success, 1 if any error occurred
    """
    log_pipeline_start("clovertex-pipeline")

    try:
        # ── Task 1: Ingestion ──────────────────────────
        print("\n" + "="*50)
        print("TASK 1: Ingesting raw data")
        print("="*50)

        from pipeline.ingestion.loader import ingest_file
        from pipeline.utils.manifest import save_manifest

        alpha_raw, _ = ingest_file(
            "data/site_alpha_patients.csv"
        )
        beta_raw, _ = ingest_file(
            "data/site_beta_patients.json"
        )
        labs_raw, _ = ingest_file(
            "data/site_gamma_lab_results.parquet"
        )
        meds_raw, _ = ingest_file(
            "data/medications_log.json"
        )
        genomics_raw, _ = ingest_file(
            "data/genomics_variants.parquet"
        )
        diagnoses_raw, _ = ingest_file(
            "data/diagnoses_icd10.csv"
        )
        notes_raw, _ = ingest_file(
            "data/clinical_notes_metadata.csv"
        )

        # Save raw zone manifest
        save_manifest("datalake/raw", "raw")
        print("Task 1 complete — raw zone ready")

        # ── Task 2: Cleaning & Refined Zone ───────────
        print("\n" + "="*50)
        print("TASK 2: Cleaning and unifying data")
        print("="*50)

        from pipeline.cleaning.clean_patients import (
            standardize_alpha,
            standardize_beta,
            unify_patients
        )
        from pipeline.cleaning.clean_general import (
            clean_medications,
            clean_lab_results,
            clean_genomics,
            clean_diagnoses,
            clean_clinical_notes
        )
        from pipeline.transformation.save_refined import (
            save_all_refined
        )

        alpha_clean, _ = standardize_alpha(alpha_raw)
        beta_clean, _ = standardize_beta(beta_raw)
        unified, _ = unify_patients(alpha_clean, beta_clean)
        labs_clean, _ = clean_lab_results(labs_raw)
        meds_clean, _ = clean_medications(meds_raw)
        genomics_clean, _ = clean_genomics(genomics_raw)
        diagnoses_clean, _ = clean_diagnoses(diagnoses_raw)
        notes_clean, _ = clean_clinical_notes(notes_raw)

        save_all_refined({
            "unified_patients":  unified,
            "lab_results":       labs_clean,
            "medications":       meds_clean,
            "genomics_variants": genomics_clean,
            "diagnoses":         diagnoses_clean,
            "clinical_notes":    notes_clean,
        })
        print("Task 2 complete — refined zone ready")

        # ── Task 3: Analytics & Consumption Zone ──────
        print("\n" + "="*50)
        print("TASK 3: Computing analytics")
        print("="*50)

        from pipeline.stats.demographics import (
            compute_demographics
        )
        from pipeline.stats.lab_stats import compute_lab_stats
        from pipeline.stats.icd10_stats import (
            compute_icd10_top15
        )
        from pipeline.stats.risk_anomaly import (
            compute_variant_hotspots,
            compute_high_risk_patients,
            detect_anomalies
        )
        from pipeline.transformation.save_consumption import (
            save_all_consumption
        )

        demographics = compute_demographics(unified)
        lab_stats = compute_lab_stats(labs_clean)
        icd10_top15 = compute_icd10_top15(diagnoses_clean)
        hotspots = compute_variant_hotspots(genomics_clean)
        high_risk = compute_high_risk_patients(
            unified, diagnoses_clean,
            genomics_clean, labs_clean
        )
        anomalies = detect_anomalies(
            unified, diagnoses_clean,
            labs_clean, meds_clean
        )

        save_all_consumption({
            "demographics_summary": demographics,
            "lab_stats":            lab_stats,
            "icd10_top15":          icd10_top15,
            "variant_hotspots":     hotspots,
            "high_risk_patients":   high_risk,
            "anomaly_flags":        anomalies,
        })
        print("Task 3 complete — consumption zone ready")

        # ── Task 4: Visualizations ─────────────────────
        print("\n" + "="*50)
        print("TASK 4: Generating visualizations")
        print("="*50)

        from pipeline.stats.plots import generate_all_plots
        generate_all_plots()
        print("Task 4 complete — all 6 plots saved")

        # ── Done ───────────────────────────────────────
        print("\n" + "="*50)
        print("PIPELINE COMPLETE!")
        print("="*50)
        log_pipeline_end("clovertex-pipeline", success=True)
        return 0

    except Exception as e:
        log_error("pipeline", str(e))
        log_pipeline_end("clovertex-pipeline", success=False)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit_code = run_pipeline()
    sys.exit(exit_code)
