-- ─────────────────────────────────────────────────────────────────────────────
-- Silver Layer — Cleansed & Validated Tax Records
--
-- Reads from bronze_tax_records and applies:
--   • Explicit type casting for every numeric / date column
--   • Null / range validation  (invalid rows excluded via WHERE)
--   • String standardisation   (UPPER + TRIM)
--   • Deduplication by (taxpayer_id, tax_year) — keeps latest filing_date
--   • Derived column: effective_tax_rate
--
-- TWO execution modes — choose one:
--
--   MODE 1 — FULL REFRESH  (default, active below)
--     CREATE MATERIALIZED VIEW rebuilt on every run.
--     run.sh wipes the ADLS warehouse before each pipeline run.
--
--   MODE 2 — SCD TYPE 1  INCREMENTAL MERGE / UPSERT  (see bottom of file)
--     MERGE INTO overwrites a matched (taxpayer_id, tax_year) row only
--     when the incoming filing_date is >= the stored value (latest wins).
--     New keys are inserted.  No history kept — SCD Type 1.
--     Pre-requisite: remove the ADLS warehouse wipe from run.sh step 6.
-- ─────────────────────────────────────────────────────────────────────────────


-- ═════════════════════════════════════════════════════════════════════════════
-- MODE 1 — Full Refresh  (active by default)
-- ═════════════════════════════════════════════════════════════════════════════

CREATE MATERIALIZED VIEW silver_tax_records AS
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
        AND filing_status IN (
            'SINGLE',
            'MARRIED_FILING_JOINTLY',
            'MARRIED_FILING_SEPARATELY',
            'HEAD_OF_HOUSEHOLD',
            'QUALIFYING_WIDOW'
        )
        AND LENGTH(state) = 2
),
ranked AS (
    -- Latest filing_date per (taxpayer_id, tax_year) gets rn = 1
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
FROM deduped;


-- ═════════════════════════════════════════════════════════════════════════════
-- MODE 2 — SCD Type 1  Incremental Merge / Upsert
--
-- To activate:
--   1. Comment out the CREATE MATERIALIZED VIEW block above.
--   2. Uncomment this entire block (remove /* and */).
--   3. In run.sh step 6, remove the ADLS warehouse wipe so that the
--      silver table persists between pipeline runs.
--
-- SCD Type 1 behaviour:
--   • MATCHED + newer/equal filing_date  → overwrite all columns (no history)
--   • MATCHED + older filing_date        → skip  (incoming is stale, ignore)
--   • NOT MATCHED                        → insert as new row
-- ═════════════════════════════════════════════════════════════════════════════

/*
MERGE INTO silver_tax_records AS target
USING (
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
            AND filing_status IN (
                'SINGLE',
                'MARRIED_FILING_JOINTLY',
                'MARRIED_FILING_SEPARATELY',
                'HEAD_OF_HOUSEHOLD',
                'QUALIFYING_WIDOW'
            )
            AND LENGTH(state) = 2
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
) AS source

ON  target.taxpayer_id = source.taxpayer_id
AND target.tax_year    = source.tax_year

-- ── SCD Type 1: overwrite when incoming record is same date or newer ──────────
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

-- ── SCD Type 1: stale incoming record — target already has a newer filing ────
-- WHEN MATCHED AND source.filing_date < target.filing_date THEN (no-op / skip)

-- ── New (taxpayer_id, tax_year) not yet in silver — insert ───────────────────
WHEN NOT MATCHED THEN
    INSERT *;
*/
