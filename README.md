# LDP Tax Pipeline — Spark Declarative Pipelines on macOS
<img width="1494" height="1008" alt="image" src="https://github.com/user-attachments/assets/175730a0-0354-49a0-a7cd-75a0b7904f27" />


A production-style **Medallion Architecture** data pipeline built with **Apache Spark 4.1 Declarative Pipelines (SDP)**, reading raw tax records from **AWS S3**, processing them through Bronze → Silver → Gold layers, persisting results to **Azure Data Lake Storage Gen2**, and publishing Gold tables to a downstream reporting store — all scheduled via cron.

---

## Table of Contents

1. [Project Intent](#1-project-intent)
2. [Architecture Overview](#2-architecture-overview)
3. [Project Structure](#3-project-structure)
4. [Prerequisites](#4-prerequisites)
5. [AWS S3 Setup](#5-aws-s3-setup)
6. [Azure ADLS Gen2 Setup](#6-azure-adls-gen2-setup)
7. [Environment Setup](#7-environment-setup)
8. [Configuration — `.env` File](#8-configuration----env-file)
9. [Download Connector JARs](#9-download-connector-jars)
10. [Running the Pipeline](#10-running-the-pipeline)
11. [Scheduling with Cron](#11-scheduling-with-cron)
12. [Querying Results — Jupyter Notebook](#12-querying-results----jupyter-notebook)
13. [Pipeline Layers in Detail](#13-pipeline-layers-in-detail)
14. [Troubleshooting](#14-troubleshooting)

---

## 1. Project Intent

Most Spark tutorials stop at "run this on Databricks." This project shows how to run a **real, cloud-connected Spark Declarative Pipeline entirely on a local macOS machine** — no cluster, no cloud VM, no Databricks workspace required.

**What it does end-to-end:**

- Reads raw US tax return CSV records from an **S3 bucket** (schema-on-read, PERMISSIVE mode)
- Ingests them into a **Bronze** materialized view with metadata stamps
- Cleanses, types, deduplicates, and derives `effective_tax_rate` into a **Silver** view
- Aggregates into two **Gold** views: annual state-level summaries and income-band distributions
- **Publishes** the Gold tables to a separate ADLS path (`rs-tax/`) for downstream consumption
- Persists all layers as **Parquet on Azure ADLS Gen2** (the Spark warehouse)
- Schedules the full run daily via **cron**
- Provides an interactive **Jupyter notebook** to query all four layers with DuckDB (no JVM needed for queries)

**Why Spark Declarative Pipelines?**

SDP (introduced in Spark 4.0) brings a Delta Live Tables-style dependency graph and incremental processing model to open-source Spark — without requiring Databricks. You define `@materialized_view` and `@append_flow` once; the framework resolves ordering, handles checkpointing, and manages retries automatically.

---

## 2. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                         AWS S3                                      │
│   s3a://<bucket>/raw/tax-records/*.csv                              │
└────────────────────────────┬────────────────────────────────────────┘
                             │ spark.read (CSV, schema-on-read)
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│  BRONZE  bronze_tax_records                                         │
│  Raw ingestion • _source_path • _ingested_at • _pipeline_run_date   │
└────────────────────────────┬────────────────────────────────────────┘
                             │ type cast • validate • dedupe
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│  SILVER  silver_tax_records                                         │
│  Typed • cleaned • deduped by (taxpayer_id, tax_year)               │
│  + effective_tax_rate = tax_liability / gross_income × 100          │
└──────────────────┬──────────────────────┬───────────────────────────┘
                   │                      │
        aggregate  │                      │  aggregate
                   ▼                      ▼
┌──────────────────────────┐  ┌──────────────────────────────────────┐
│  GOLD 1                  │  │  GOLD 2                              │
│  gold_tax_annual_summary │  │  gold_tax_income_bands               │
│  By (year, state,        │  │  By (year, income_band,              │
│      filing_status)      │  │      effective_rate_band)            │
└──────────┬───────────────┘  └──────────────┬────────────────────── ┘
           │                                 │
           └──────────────┬──────────────────┘
                          │ streaming append_flow
                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│  ADLS  abfs://medallion@<account>.dfs.core.windows.net/rs-tax/      │
│  Parquet snapshots ready for Power BI / downstream consumers        │
└─────────────────────────────────────────────────────────────────────┘

Warehouse: abfs://medallion@<account>.dfs.core.windows.net/ldp-tax/warehouse/
```

---

## 3. Project Structure

```
SparkSDP/
├── pipelines/
│   ├── 01_bronze_tax.py                # Bronze — S3 CSV ingestion
│   ├── 02_silver_tax.sql               # Silver — cleanse, type, dedupe
│   ├── 03_gold_tax_annual_summary.sql  # Gold 1 — annual state summary
│   ├── 04_gold_tax_income_bands.sql    # Gold 2 — income band distribution
│   └── 05_publish_rs_tax_sdp.py        # Publish Gold → rs-tax (streaming)
├── sample_data/
│   ├── tax_records_2024_00.csv          # Base 2023–2024 sample (25 rows)
│   ├── tax_records_incremental_1.csv    # Incremental batch 1 — 2024–2025 (13 rows)
│   └── tax_records_incremental_2.csv    # Incremental batch 2 — 2025–2026 + amended (10 rows)
├── jars/                               # Connector JARs (gitignored)
│   ├── hadoop-aws-3.4.2.jar
│   ├── bundle-2.26.0.jar
│   └── hadoop-azure-3.4.2.jar
├── logs/                               # Pipeline run logs (gitignored)
├── LDP_Tax_Queries.ipynb               # Interactive query notebook
├── spark-pipeline-template.yml         # Pipeline spec template
├── spark-pipeline.yml                  # Generated at runtime by run.sh
├── run.sh                              # Main pipeline entrypoint
├── download_jars.sh                    # One-time JAR installer
├── setup_cron.sh                       # Cron job installer/remover
└── .env                                # Credentials (never commit)
```

---

## 4. Prerequisites

| Requirement | Version | Install |
|---|---|---|
| macOS | 13+ (Apple Silicon or Intel) | — |
| Apache Spark | **4.1.1** | `brew install apache-spark` |
| Java (Temurin) | **17** | `brew install --cask temurin@17` |
| Python | 3.10+ | `brew install pyenv` |
| AWS CLI | any | `brew install awscli` (for S3 setup only) |

> **Important — Java 17 is required.** Spark 4.1 with Hadoop 3.4 uses `javax.security.auth.Subject.getSubject()`, which is blocked in Java 18+. The pipeline pins Java 17 explicitly in `run.sh` so cron and interactive runs behave identically.

### Install Spark via Homebrew

```bash
brew install apache-spark
```

Verify:

```bash
spark-submit --version
# Apache Spark version 4.1.1
```

### Install Python Packages

`spark-pipelines` is bundled with Homebrew Spark (PySpark 4.1.1 is included) — no separate venv is needed to run the pipeline.

The notebook (`LDP_Tax_Queries.ipynb`) queries data via DuckDB and needs a few packages. Install them into whichever Python your notebook kernel uses:

```bash
pip install adlfs pyarrow pandas duckdb jupyter
```

> If you do want to isolate dependencies in a venv, create one and set `VENV_PATH` in `run.sh` before the `source` line, or export it before running:
> ```bash
> export VENV_PATH=/path/to/your/.venv
> ./run.sh
> ```

---

## 5. AWS S3 Setup

The Bronze layer reads raw CSV tax records from S3.

### 5.1 Create the Bucket

```bash
aws s3 mb s3://my-company-tax-data --region us-east-1
```

### 5.2 Upload Sample Data

```bash
aws s3 cp sample_data/ s3://my-company-tax-data/raw/tax-records/ --recursive
```

### 5.3 Create an IAM User & Access Key

1. Go to **IAM → Users → Create user**
2. Attach policy: `AmazonS3ReadOnlyAccess` (or a custom policy scoped to your bucket)
3. Under **Security credentials**, create an **Access key**
4. Save the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`

### 5.4 Required S3 Permissions (least-privilege)

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::my-company-tax-data",
        "arn:aws:s3:::my-company-tax-data/raw/tax-records/*"
      ]
    }
  ]
}
```

---

## 6. Azure ADLS Gen2 Setup

The pipeline writes all Parquet output — the Spark warehouse and the published `rs-tax` tables — to ADLS Gen2.

### 6.1 Create a Storage Account

1. In the Azure Portal, go to **Storage accounts → Create**
2. Set **Hierarchical namespace** to **Enabled** (this makes it ADLS Gen2)
3. Note the **Storage account name** (e.g. `mytaxdatalake`)

### 6.2 Create the Container

```bash
az storage container create \
  --name medallion \
  --account-name mytaxdatalake
```

Or via Portal: **Storage account → Containers → + Container**, name it `medallion`.

### 6.3 Get the Account Key

Portal: **Storage account → Security + networking → Access keys → key1 → Show**

Or via CLI:

```bash
az storage account keys list \
  --account-name mytaxdatalake \
  --query "[0].value" -o tsv
```

### 6.4 ADLS Directory Layout (created automatically by the pipeline)

```
medallion/
├── ldp-tax/
│   ├── checkpoints/        ← SDP streaming checkpoints
│   └── warehouse/
│       ├── bronze_tax_records/
│       ├── silver_tax_records/
│       ├── gold_tax_annual_summary/
│       └── gold_tax_income_bands/
└── rs-tax/
    ├── gold_tax_annual_summary/    ← published for downstream consumers
    ├── gold_tax_income_bands/
    └── _checkpoints/
```

---

## 7. Environment Setup

Clone the repo and prepare the runtime directories:

```bash
git clone <repo-url>
cd SparkSDP
mkdir -p logs jars
cp .env.example .env   # then fill in your real values
```

---

## 8. Configuration — `.env` File

Create `.env` in the project root. **Never commit this file.**

```bash
# ── AWS S3 Source ──────────────────────────────────────────────────
AWS_ACCESS_KEY_ID=AKIAXXXXXXXXXXXXXXXX
AWS_SECRET_ACCESS_KEY=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
AWS_REGION=us-east-1

# S3 source path: s3a://<S3_BUCKET>/<S3_PREFIX>/
S3_BUCKET=my-company-tax-data
S3_PREFIX=raw/tax-records

# ── Azure ADLS Gen2 Destination ────────────────────────────────────
ADLS_ACCOUNT_NAME=mytaxdatalake
ADLS_ACCOUNT_KEY=base64encodedkeyhere==

# ── Pipeline Behaviour ─────────────────────────────────────────────
# Set to "true" to use local CSV instead of S3 (offline testing)
LOCAL_MODE=false

# Local fallback path — directory containing CSV files (used when LOCAL_MODE=true)
LOCAL_SAMPLE_DATA=/Volumes/D/Projects/SparkSDP/sample_data/
```

| Variable | Description |
|---|---|
| `AWS_ACCESS_KEY_ID` | IAM access key for S3 reads |
| `AWS_SECRET_ACCESS_KEY` | IAM secret key |
| `AWS_REGION` | S3 bucket region |
| `S3_BUCKET` | Source bucket name |
| `S3_PREFIX` | Prefix inside the bucket (no leading/trailing `/`) |
| `ADLS_ACCOUNT_NAME` | Azure Storage account name |
| `ADLS_ACCOUNT_KEY` | Azure Storage account key (Base64) |
| `LOCAL_MODE` | `true` = read local CSV, write to `./spark-warehouse` |
| `LOCAL_SAMPLE_DATA` | Absolute path to local CSV directory (all CSVs are read; used when `LOCAL_MODE=true`) |

---

## 9. Download Connector JARs

Spark's Homebrew installation does not include the S3 or ADLS connectors. Run this **once**:

```bash
./download_jars.sh
```

This downloads and installs three JARs into both `./jars/` and `$SPARK_HOME/jars/`:

| JAR | Purpose |
|---|---|
| `hadoop-aws-3.4.2.jar` | S3A filesystem connector |
| `bundle-2.26.0.jar` | AWS SDK v2 (required by hadoop-aws 3.4.2) |
| `hadoop-azure-3.4.2.jar` | ABFS connector for ADLS Gen2 |

> These must match Spark 4.1's bundled `hadoop-client-runtime-3.4.2`.

---

## 10. Running the Pipeline

### Cloud Mode (default)

```bash
./run.sh
```

What happens under the hood:

1. Loads `.env` credentials
2. Writes a runtime `spark-defaults.conf` with ADLS + S3 keys (injected before the JVM starts)
3. Generates `spark-pipeline.yml` from the template
4. Wipes `metastore_db` and the ADLS warehouse (full-refresh design)
5. Activates the Python venv
6. Runs `spark-pipelines run --spec spark-pipeline.yml`
7. Executes all 6 flows in dependency order: Bronze → Silver → Gold 1 → Gold 2 → Publish × 2
8. Cleans up the runtime conf (contains secrets)

Expected output:

```
[INFO] CLOUD MODE — warehouse: abfs://medallion@mytaxdatalake.dfs.core.windows.net/ldp-tax/warehouse
...
Flow bronze_tax_records             → COMPLETED
Flow silver_tax_records             → COMPLETED
Flow gold_tax_annual_summary        → COMPLETED
Flow gold_tax_income_bands          → COMPLETED
Flow publish__gold_tax_annual_summary → COMPLETED
Flow publish__gold_tax_income_bands   → COMPLETED
Run is COMPLETED.
[SUCCESS] Pipeline completed
```

### Local / Offline Mode

```bash
LOCAL_MODE=true ./run.sh
```

Reads all CSV files from `sample_data/`, writes to `./spark-warehouse` — no AWS or Azure credentials needed.

### Force Full Recompute

```bash
./run.sh --full-refresh-all
```

---

## 11. Scheduling with Cron

The `setup_cron.sh` script installs or removes the cron job.

### Install (default schedule: 17:16 daily)

```bash
./setup_cron.sh
```

### Install with a Custom Schedule

```bash
# Daily at 06:00
CRON_SCHEDULE="0 6 * * *" ./setup_cron.sh

# Every 6 hours
CRON_SCHEDULE="0 */6 * * *" ./setup_cron.sh

# Weekdays at 08:30
CRON_SCHEDULE="30 8 * * 1-5" ./setup_cron.sh
```

### Remove the Cron Job

```bash
REMOVE_CRON=true ./setup_cron.sh
```

### View the Installed Entry

```bash
crontab -l
# 16 17 * * * /Volumes/D/Projects/SparkSDP/run.sh >> .../logs/cron.log 2>&1
```

### Monitor Logs

```bash
tail -f logs/cron.log
```

> **Timezone note:** cron runs in the system's local timezone. The pipeline pins `JAVA_HOME` to Java 17 inside `run.sh` to ensure cron picks up the correct JVM regardless of the shell environment.

---

## 12. Querying Results — `LDP_Tax_Queries.ipynb`

### Why DuckDB instead of PySpark for queries?

Once the pipeline has written Parquet to ADLS, you don't need a JVM or a Spark session to explore the data. **DuckDB** reads Parquet files directly from ADLS via `adlfs` + `pyarrow` — it starts in milliseconds, uses zero Java memory, and supports full SQL. The notebook wraps DuckDB in a thin `spark.sql(...)` shim so query cells look identical to what you'd write in a Databricks notebook, making it easy to port queries either direction.

```
Pipeline (Spark JVM)  →  Parquet on ADLS  →  DuckDB (no JVM)  →  pandas DataFrame
```

### Setup

Install query dependencies into whichever Python your notebook kernel uses:

```bash
pip install duckdb adlfs pyarrow pandas
```

### Open the Notebook

Open `LDP_Tax_Queries.ipynb` in **Jupyter Lab**, **VS Code**, or **Cursor**. Connect a Python kernel, then run the setup cell at the top.

### Mode Toggle

At the top of the setup cell, set `MODE`:

```python
# "cloud" → reads Parquet from Azure ADLS Gen2 (requires .env credentials)
# "local" → reads Parquet from ./spark-warehouse  (after LOCAL_MODE=true ./run.sh)
MODE = "cloud"
```

The setup cell reads credentials from the project `.env`, connects to ADLS via `adlfs.AzureBlobFileSystem`, loads all four tables into DuckDB, and prints a confirmation:

```
[OK] bronze_tax_records        (38 rows)
[OK] silver_tax_records        (36 rows)
[OK] gold_tax_annual_summary   (31 rows)
[OK] gold_tax_income_bands     (19 rows)

DuckDB 1.4.4 (no JVM) | mode=cloud
Warehouse : abfs://medallion@mytaxdatalake.dfs.core.windows.net/ldp-tax/warehouse
```

### Spark-Compatible SQL Wrapper

The notebook exposes a `spark` object that mimics the PySpark API:

```python
spark.sql("""
    SELECT state, SUM(tax_liability) AS total_tax
    FROM   silver_tax_records
    GROUP BY state
    ORDER BY total_tax DESC
""").show(10)
```

Output renders as a formatted pandas table — no `.show()` truncation quirks, no JVM overhead.

### Available Query Sections

| Section | Notebook cells | What it shows |
|---|---|---|
| **Bronze** | 2 | All raw ingested records; row count, date range, distinct states |
| **Silver** | 5 | Effective tax rates; quality summary by year; top earners; balance-due and refund analysis; YoY comparison |
| **Gold 1** | 4 | Full annual summary; top states by tax collected; filing-status breakdown; states with >50% balance-due |
| **Gold 2** | 3 | Income band breakdown by year; total tax per band; each band's share of total tax |
| **Ad-hoc** | 2 | Editable scratch cells — write any SQL against all four registered tables |

### Ad-hoc Query Example

```python
# Edit and run — all four tables are registered in DuckDB
spark.sql("""
    SELECT taxpayer_id, taxpayer_name, state, tax_year,
           gross_income, effective_tax_rate
    FROM   silver_tax_records
    WHERE  state = 'CA'
    ORDER BY effective_tax_rate DESC
""").show(20, truncate=False)
```

### Cleanup

The last cell closes the DuckDB in-memory connection:

```python
con.close()
```

---

## 13. Pipeline Layers in Detail

### Python vs SQL — design note

Bronze and Publish layers are written in Python (`@dp.materialized_view`, `@dp.append_flow`). Silver and Gold must be SQL (`CREATE MATERIALIZED VIEW`). This is a constraint of SDP 4.1.1:

- **Python flows** (`DefineFlow`) are registered by serialising the PySpark plan client-side, then sending it to the Spark Connect server. The server validates all table references against the **live Hive catalog**. Because the warehouse is wiped at pipeline startup, pipeline-registered tables don't exist in the catalog at registration time → `TABLE_OR_VIEW_NOT_FOUND`.
- **SQL files** (`DefineSqlGraphElements`) send raw SQL text to the server, which resolves table names inside the **pipeline graph context** — pipeline-registered tables are always resolvable, regardless of the catalog.

Rule of thumb: use Python for layers that read **external sources** (S3, CSV, cloud storage) or write **sinks**. Use SQL for layers that read from **other pipeline tables**.

---

### Bronze — `01_bronze_tax.py`

- Reads all CSVs from S3 (or the local `sample_data/` directory in `LOCAL_MODE`)
- Uses `inferSchema=true`, `mode=PERMISSIVE` — malformed rows come through with nulls
- Stamps every row with `_source_path`, `_ingested_at`, `_pipeline_run_date`

### Silver — `02_silver_tax.sql`

- Explicit `CAST` for every numeric and date column
- Excludes rows where `taxpayer_id IS NULL`, `tax_year` out of range, negative amounts, or invalid `filing_status` / `state`
- Deduplicates by `(taxpayer_id, tax_year)` keeping the latest `filing_date`
- Derives `effective_tax_rate = ROUND(tax_liability / gross_income * 100, 2)`

### Gold 1 — `03_gold_tax_annual_summary.sql`

Grain: `(tax_year, state, filing_status)`

Key metrics: `total_gross_income`, `total_tax_liability`, `avg_effective_tax_rate`, `total_balance_due`, `total_refund_amount`, `pct_with_balance_due`

### Gold 2 — `04_gold_tax_income_bands.sql`

Grain: `(tax_year, income_band, effective_rate_band)`

Groups taxpayers into income brackets and effective-rate bands for distribution analysis.

### Publish — `05_publish_rs_tax_sdp.py`

Uses `dp.create_sink` + `@dp.append_flow` to stream both Gold tables to `abfs://medallion@<account>.dfs.core.windows.net/rs-tax/` as Parquet. Reads from the pipeline's own table registry (`spark.readStream.table(t)`) so the dependency on Gold completion is enforced by the framework.

---

## 14. Troubleshooting

### `getSubject is not supported`

**Cause:** Running with Java 18+ (Hadoop uses a method removed in newer JVMs).
**Fix:** Ensure Java 17 is active. `run.sh` pins it automatically via `JAVA_HOME`.

```bash
java -version   # should show 17.x
```

### `PATH_NOT_FOUND` warnings during publish flows

**Cause:** The streaming checkpoint remembers parquet file names from the previous run. Since the warehouse is wiped on every run, those files no longer exist. Spark recovers gracefully.
**Impact:** None — the pipeline completes successfully. Can be eliminated by clearing `rs-tax/_checkpoints` before each run.

### `LOCATION_ALREADY_EXISTS` on re-run

**Cause:** Stale `metastore_db` out of sync with the warehouse.
**Fix:** `run.sh` automatically removes `metastore_db` before every cloud run. If it persists, delete manually: `rm -rf metastore_db`.

### Missing JARs error

```
[ERROR] Cloud mode requires JARs in .../jars/
```

**Fix:** Run `./download_jars.sh` first.

### Cron job runs with wrong Java

**Cause:** cron does not source your shell profile so `JAVA_HOME` is unset.
**Fix:** Already handled — `run.sh` exports `JAVA_HOME` to Temurin 17 at the top of the script. If you installed Java 17 to a different path, update that line in `run.sh`.

### `ADLS_ACCOUNT_NAME and ADLS_ACCOUNT_KEY must be set`

**Cause:** `.env` is missing or the variables are not set.
**Fix:** Verify `.env` exists in the project root and contains both variables. The file must not have extra spaces around `=`.

---

## .gitignore Recommendations

```gitignore
.env
jars/
logs/
metastore_db/
spark-warehouse/
.spark-conf-runtime/
__pycache__/
.venv/
```

> `spark-pipeline.yml` is **committed** to source control — it contains ADLS account name but no credentials (those are injected at runtime via `SPARK_CONF_DIR`). The `.gitignore` intentionally does not exclude it.

---

*Built with Apache Spark 4.1.1 · Spark Declarative Pipelines · AWS S3 · Azure ADLS Gen2 · DuckDB · macOS*
