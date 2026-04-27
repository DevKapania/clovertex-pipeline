# Clovertex Clinical Data Pipeline

A production-grade data pipeline that ingests multi-format
clinical and genomics data from 3 hospital sites, cleans and
unifies it, organizes it into a 3-zone data lake, computes
analytics, and generates 6 visualizations.

---

## Quick Start

```bash
docker compose up
```

That single command builds the image, runs all 4 tasks,
and writes all outputs to `datalake/` on your local machine.

---

## Architecture Overview
data/                          ← Raw input files (read-only)
├── site_alpha_patients.csv    ← 370 patients (Site Alpha)
├── site_beta_patients.json    ← 310 patients (Site Beta, nested)
├── site_gamma_lab_results.parquet  ← 2026 lab results
├── medications_log.json       ← 1922 medication records
├── genomics_variants.parquet  ← 1137 genomic variants
├── diagnoses_icd10.csv        ← 1638 diagnoses
├── clinical_notes_metadata.csv ← 1128 clinical notes
└── reference/
├── lab_test_ranges.json   ← Normal lab value ranges
├── icd10_chapters.csv     ← ICD-10 chapter mappings
└── gene_reference.json    ← Gene reference data
pipeline/                      ← All Python source code
├── main.py                    ← Master orchestrator
├── ingestion/                 ← File loaders (CSV/JSON/Parquet)
├── cleaning/                  ← Data cleaning modules
├── transformation/            ← Zone writers + manifests
├── stats/                     ← Analytics + plots
└── utils/                     ← Logger + manifest generator
datalake/                      ← All pipeline outputs
├── raw/                       ← Original files archived
│   └── manifest.json
├── refined/                   ← Cleaned Parquet files
│   ├── unified_patients.parquet
│   ├── lab_results/           ← Partitioned by site_name
│   ├── medications.parquet
│   ├── genomics_variants.parquet
│   ├── diagnoses.parquet
│   ├── clinical_notes.parquet
│   └── manifest.json
└── consumption/               ← Analytics outputs
├── demographics_summary.parquet
├── lab_stats.parquet
├── icd10_top15.parquet
├── variant_hotspots.parquet
├── high_risk_patients.parquet
├── anomaly_flags.parquet
├── manifest.json
└── plots/                 ← 6 PNG visualizations
├── plot1_demographics.png
├── plot2_icd10_top15.png
├── plot3_lab_distributions.png
├── plot4_genomics_scatter.png
├── plot5_high_risk_summary.png
└── plot6_data_quality.pngdata/                          ← Raw input files (read-only)
├── site_alpha_patients.csv    ← 370 patients (Site Alpha)
├── site_beta_patients.json    ← 310 patients (Site Beta, nested)
├── site_gamma_lab_results.parquet  ← 2026 lab results
├── medications_log.json       ← 1922 medication records
├── genomics_variants.parquet  ← 1137 genomic variants
├── diagnoses_icd10.csv        ← 1638 diagnoses
├── clinical_notes_metadata.csv ← 1128 clinical notes
└── reference/
├── lab_test_ranges.json   ← Normal lab value ranges
├── icd10_chapters.csv     ← ICD-10 chapter mappings
└── gene_reference.json    ← Gene reference data
pipeline/                      ← All Python source code
├── main.py                    ← Master orchestrator
├── ingestion/                 ← File loaders (CSV/JSON/Parquet)
├── cleaning/                  ← Data cleaning modules
├── transformation/            ← Zone writers + manifests
├── stats/                     ← Analytics + plots
└── utils/                     ← Logger + manifest generator
datalake/                      ← All pipeline outputs
├── raw/                       ← Original files archived
│   └── manifest.json
├── refined/                   ← Cleaned Parquet files
│   ├── unified_patients.parquet
│   ├── lab_results/           ← Partitioned by site_name
│   ├── medications.parquet
│   ├── genomics_variants.parquet
│   ├── diagnoses.parquet
│   ├── clinical_notes.parquet
│   └── manifest.json
└── consumption/               ← Analytics outputs
├── demographics_summary.parquet
├── lab_stats.parquet
├── icd10_top15.parquet
├── variant_hotspots.parquet
├── high_risk_patients.parquet
├── anomaly_flags.parquet
├── manifest.json
└── plots/                 ← 6 PNG visualizations
├── plot1_demographics.png
├── plot2_icd10_top15.png
├── plot3_lab_distributions.png
├── plot4_genomics_scatter.png
├── plot5_high_risk_summary.png
└── plot6_data_quality.png

---

## Data Lake Zones Explained

### Raw Zone (`datalake/raw/`)
**Purpose:** Permanent archive of original files, never modified.
Every source file is copied here untouched. This is the
"source of truth" — if anything goes wrong downstream,
we can always reprocess from raw.

**What is stored:** Original CSV, JSON, and Parquet files.
**Manifest:** Tracks filename, size, SHA-256 checksum, timestamp.

### Refined Zone (`datalake/refined/`)
**Purpose:** Cleaned, standardized, unified data ready for analytics.
All files saved as Parquet for performance and type safety.

**What is stored:** Cleaned DataFrames with:
- Unified patient table (Alpha + Beta combined)
- Lab results partitioned by `site_name`
- All dates in ISO 8601 format (YYYY-MM-DD)
- Consistent column names across all datasets

**Why partition lab results?**
Lab results (2026 rows) are the largest dataset and are
frequently queried by site. Partitioning means queries
for one site read only that partition — much faster.

**Manifest:** Tracks filename, row count, schema,
SHA-256 checksum, ISO timestamp per file.

### Consumption Zone (`datalake/consumption/`)
**Purpose:** Analytics-ready outputs and visualizations.
These are the finished products — ready for dashboards,
reports, and regulatory submissions.

**What is stored:** 6 analytics Parquet files + 6 PNG plots.

---

## Cleaning Decisions

### Patient Unification
- Site Alpha (CSV): 370 patients, flat structure
- Site Beta (JSON): 310 patients, deeply nested structure
  - `name.given` / `name.family` → `first_name` / `last_name`
  - `encounter.admissionDate` → `admission_dt`
  - `contact.phone` → `contact_phone`
- **30 duplicate patient IDs removed** after union
- All dates standardized from MM/DD/YYYY to YYYY-MM-DD

### Lab Results
- Renamed `patient_ref` → `patient_id` to match master table
- **31 negative test values** replaced with NaN (impossible values)
- Collection dates normalized to ISO format

### Medications
- **35 empty dosage strings** (`""`) converted to NaN
- **3,026 date values** normalized across 10+ input formats:
  - `"August 31, 2023"` → `"2023-08-31"`
  - `"29-03-2022"` → `"2022-03-29"`
  - `"2022-07-03T00:00:00"` → `"2022-07-03"`
- Dosage casing standardized: `"1000MG"` → `"1000mg"`

### Genomics Variants
- Renamed `patient_ref` → `patient_id`
- **42 low read-depth variants filtered** (read_depth < 10)
  - Reason: read_depth < 10 means sequencing is unreliable
  - This is the standard bioinformatics quality threshold
- All allele frequencies validated (must be 0.0–1.0)

### Diagnoses
- Dates normalized to ISO format
- Severity and status standardized to lowercase
- `is_primary` converted from Y/N to boolean

---

## Analytics Produced

### Demographics Summary
Age statistics (mean, median, min, max, std), age group
distribution (0-17, 18-35, 36-60, 60+), sex distribution,
blood group distribution, site distribution.

### Lab Statistics
Per test type: count, mean, median, std, min, max,
plus reference range flags showing % below/above/in range.
16 test types analyzed across 2,026 results.

### Top 15 ICD-10 Chapters
Maps each diagnosis code to its ICD-10 chapter and counts
unique patients per chapter. Identifies dominant disease
burden across the population.

### Variant Hotspots
58 genes analyzed. Shows variant count, patient count,
pathogenic variant count, and average allele frequency
per gene. Identifies oncology risk genes.

### High Risk Patients
**403 of 650 patients flagged** using multi-criteria scoring:
- Pathogenic/Likely Pathogenic genomic variant
- 3 or more diagnoses
- Extreme lab values (>3x reference maximum)
- Severe primary diagnosis

Risk levels: 3 critical, 36 high, 364 moderate.

### Anomaly Flags
**25 anomalies detected** across the dataset:
- 12 critical (lab values incompatible with life)
- 13 high (discharge before admission dates)
- 0 moderate

---

## Visualizations

| Plot | File | Description |
|------|------|-------------|
| 1 | plot1_demographics.png | Age histogram + sex bar chart |
| 2 | plot2_icd10_top15.png | Top 15 ICD-10 chapters horizontal bar |
| 3 | plot3_lab_distributions.png | KDE plots for 4 test types with reference lines |
| 4 | plot4_genomics_scatter.png | Allele frequency vs read depth by significance |
| 5 | plot5_high_risk_summary.png | Risk level distribution + top risk factors |
| 6 | plot6_data_quality.png | Pipeline quality metrics bar chart |

---

## Running Locally (Without Docker)

```bash
# Clone and enter project
cd /path/to/clovertex-pipeline

# Create virtual environment
python3.11 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run pipeline
python -m pipeline.main
```

---

## DevOps

### Docker
- Base image: `python:3.11-slim`
- Multi-layer caching: requirements installed before code copy
- Volume mount: `./datalake:/app/datalake` ensures outputs
  persist after container exits
- Exit code 0 = success, 1 = failure

### GitHub Actions CI (`ci.yml`)
Runs on every push:
1. **Lint** — `ruff check pipeline/` and `ruff format --check`
2. **Docker build** — verifies image builds successfully

CI must be green on `main` branch.

---

## Key Findings

| Metric | Value |
|--------|-------|
| Total patients unified | 650 |
| Duplicates removed | 30 |
| Lab results processed | 2,026 |
| Negative lab values fixed | 31 |
| Genomic variants after filter | 1,095 / 1,137 |
| Medication date formats normalized | 3,026 |
| High risk patients identified | 403 |
| Clinical anomalies detected | 25 |
| Genes analyzed | 58 |
| ICD-10 chapters mapped | 13 |

---

## Tech Stack

| Tool | Version | Purpose |
|------|---------|---------|
| Python | 3.11 | Core language |
| pandas | 2.2.2 | Data manipulation |
| pyarrow | 15.0.2 | Parquet I/O |
| matplotlib | 3.8.4 | Visualizations |
| seaborn | 0.13.2 | Plot styling |
| dateutil | 2.9.0 | Date parsing |
| Docker | 24+ | Containerization |
| ruff | latest | Linting/formatting |
