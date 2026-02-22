# ─────────────────────────────────────────────────────────────────────────────
# Bronze Layer — Raw Tax Data Ingestion from S3
#
# Reads raw CSV tax records from S3 (or local sample in LOCAL_MODE),
# adds ingestion metadata, and materialises the result as-is.
# No business transformations here — only ingest and stamp.
# ─────────────────────────────────────────────────────────────────────────────
import os
from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

spark = SparkSession.active()

# ── Source path ───────────────────────────────────────────────────────────────
_LOCAL_MODE = os.environ.get("LOCAL_MODE", "false").lower() == "true"

if _LOCAL_MODE:
    _SOURCE_PATH = os.environ.get(
        "LOCAL_SAMPLE_DATA",
        "/Volumes/D/Projects/PysparkLocal/SDP-Cron/sample_data/",
    )
    _READ_FORMAT = "csv"
else:
    _BUCKET = os.environ["S3_BUCKET"]
    _PREFIX = os.environ.get("S3_PREFIX", "raw/tax-records").strip("/")
    _SOURCE_PATH = f"s3a://{_BUCKET}/{_PREFIX}/"
    _READ_FORMAT = "csv"


# ── Bronze materialized view ──────────────────────────────────────────────────
@dp.materialized_view(
    comment="Raw tax records ingested from S3 — schema-on-read, no transforms",
)
def bronze_tax_records() -> DataFrame:
    return (
        spark.read
        .format(_READ_FORMAT)
        .option("header", "true")
        .option("inferSchema", "true")
        .option("multiLine", "false")
        .option("mode", "PERMISSIVE")          # keep bad rows with nulls
        .load(_SOURCE_PATH)
        # ── Ingestion metadata ────────────────────────────────────────────────
        .withColumn("_source_path",        F.input_file_name())
        .withColumn("_ingested_at",        F.current_timestamp())
        .withColumn("_pipeline_run_date",  F.current_date())
    )
