-- ─────────────────────────────────────────────────────────────────────────────
-- Silver Layer — Cleansed & Validated Tax Records
--
-- Reads from bronze_tax_records and applies:
--   • Explicit type casting for every numeric / date column
--   • Null / range validation (invalid rows excluded via WHERE)
--   • String standardisation  (UPPER + TRIM)
--   • Deduplication by (taxpayer_id, tax_year) — keeps latest filing_date
--   • Derived column: effective_tax_rate
-- ─────────────────────────────────────────────────────────────────────────────

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
        -- Effective tax rate (%)
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
    -- Assign row number; latest filing_date per (taxpayer_id, tax_year) gets rn=1
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
