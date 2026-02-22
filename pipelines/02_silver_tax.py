# ─────────────────────────────────────────────────────────────────────────────
# Silver Layer — Cleansed & Validated Tax Records  (SCD Type 1)
#
# Reads from bronze_tax_records and applies:
#   • Explicit type casting for every numeric / date column
#   • Null / range validation  (invalid rows excluded via WHERE)
#   • String standardisation   (UPPER + TRIM)
#   • Deduplication by (taxpayer_id, tax_year) — keeps latest filing_date
#   • Derived column: effective_tax_rate
#
# SCD Type 1 behaviour (via MERGE INTO):
#   • MATCHED + newer/equal filing_date  → overwrite all columns (no history)
#   • MATCHED + older filing_date        → skip  (incoming is stale, ignore)
#   • NOT MATCHED                        → insert as new row
#
# NOTE: MERGE INTO is not supported in Spark Pipelines SQL files — this logic
# must live in a Python file using spark.sql() directly.
# ─────────────────────────────────────────────────────────────────────────────
from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession

spark = SparkSession.active()

_VALID_FILING_STATUSES = (
    "SINGLE",
    "MARRIED_FILING_JOINTLY",
    "MARRIED_FILING_SEPARATELY",
    "HEAD_OF_HOUSEHOLD",
    "QUALIFYING_WIDOW",
)

_TRANSFORM_SQL = """
WITH typed AS (
    SELECT
        TRIM(taxpayer_id)                               AS taxpayer_id,
        TRIM(tin)                                       AS tin,
        TRIM(taxpayer_name)                             AS taxpayer_name,
        CAST(tax_year            AS INT)                AS tax_year,
        UPPER(TRIM(filing_status))                      AS filing_status,
        UPPER(TRIM(state))                              AS state,
        CAST(gross_income        AS DOUBLE)             AS gross_income,
        CAST(wages               AS DOUBLE)             AS wages,
        CAST(interest_income     AS DOUBLE)             AS interest_income,
        CAST(dividend_income     AS DOUBLE)             AS dividend_income,
        CAST(business_income     AS DOUBLE)             AS business_income,
        CAST(deductions          AS DOUBLE)             AS deductions,
        CAST(taxable_income      AS DOUBLE)             AS taxable_income,
        CAST(tax_liability       AS DOUBLE)             AS tax_liability,
        CAST(tax_withheld        AS DOUBLE)             AS tax_withheld,
        CAST(estimated_payments  AS DOUBLE)             AS estimated_payments,
        CAST(tax_paid            AS DOUBLE)             AS tax_paid,
        CAST(balance_due         AS DOUBLE)             AS balance_due,
        CAST(refund_amount       AS DOUBLE)             AS refund_amount,
        TO_DATE(filing_date, 'yyyy-MM-dd')              AS filing_date,
        UPPER(TRIM(return_type))                        AS return_type,
        _ingested_at,
        _pipeline_run_date
    FROM bronze_tax_records
),
validated AS (
    SELECT *,
        CASE
            WHEN gross_income > 0
            THEN ROUND(tax_liability / gross_income * 100, 2)
            ELSE 0.0
        END AS effective_tax_rate
    FROM typed
    WHERE
        taxpayer_id   IS NOT NULL
        AND tax_year  BETWEEN 1990 AND 2030
        AND gross_income   >= 0
        AND taxable_income >= 0
        AND tax_liability  >= 0
        AND filing_status  IN {statuses}
        AND LENGTH(state)  = 2
),
ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY taxpayer_id, tax_year
               ORDER BY filing_date DESC NULLS LAST
           ) AS _rn
    FROM validated
),
deduped AS (
    SELECT * EXCEPT (_rn) FROM ranked WHERE _rn = 1
)
SELECT
    taxpayer_id,
    tin,
    taxpayer_name,
    tax_year,
    filing_status,
    state,
    gross_income,
    wages,
    interest_income,
    dividend_income,
    business_income,
    deductions,
    taxable_income,
    tax_liability,
    tax_withheld,
    estimated_payments,
    tax_paid,
    balance_due,
    refund_amount,
    filing_date,
    return_type,
    effective_tax_rate,
    _ingested_at,
    _pipeline_run_date,
    CURRENT_TIMESTAMP() AS _processed_at
FROM deduped
""".format(statuses=str(_VALID_FILING_STATUSES))

_MERGE_SQL = """
MERGE INTO silver_tax_records AS target
USING _silver_incoming AS source

ON  target.taxpayer_id = source.taxpayer_id
AND target.tax_year    = source.tax_year

-- SCD Type 1: overwrite when incoming record is same date or newer
WHEN MATCHED AND source.filing_date >= target.filing_date THEN
    UPDATE SET
        tin                = source.tin,
        taxpayer_name      = source.taxpayer_name,
        filing_status      = source.filing_status,
        state              = source.state,
        gross_income       = source.gross_income,
        wages              = source.wages,
        interest_income    = source.interest_income,
        dividend_income    = source.dividend_income,
        business_income    = source.business_income,
        deductions         = source.deductions,
        taxable_income     = source.taxable_income,
        tax_liability      = source.tax_liability,
        tax_withheld       = source.tax_withheld,
        estimated_payments = source.estimated_payments,
        tax_paid           = source.tax_paid,
        balance_due        = source.balance_due,
        refund_amount      = source.refund_amount,
        filing_date        = source.filing_date,
        return_type        = source.return_type,
        effective_tax_rate = source.effective_tax_rate,
        _ingested_at       = source._ingested_at,
        _pipeline_run_date = source._pipeline_run_date,
        _processed_at      = source._processed_at

-- New (taxpayer_id, tax_year) not yet in silver — insert
WHEN NOT MATCHED THEN
    INSERT *
"""


@dp.materialized_view(comment="Silver — cleansed, typed, deduped; SCD1 merge on re-runs")
def silver_tax_records() -> DataFrame:
    incoming: DataFrame = spark.sql(_TRANSFORM_SQL)

    if spark.catalog.tableExists("silver_tax_records"):
        # Subsequent runs: SCD Type 1 merge — register incoming as a temp view
        # then execute MERGE INTO so the framework's table is updated in place.
        incoming.createOrReplaceTempView("_silver_incoming")
        spark.sql(_MERGE_SQL)
        # Return the current state of the table so the framework can track lineage.
        return spark.table("silver_tax_records")

    # Initial run: table does not exist yet — return full incoming dataset.
    return incoming
