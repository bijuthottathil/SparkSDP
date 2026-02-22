-- ─────────────────────────────────────────────────────────────────────────────
-- Gold Layer 2 — Taxpayer Distribution by Income & Effective-Rate Bands
--
-- Business use: tax policy analysis, bracket studies, compliance insights.
-- Grain: (tax_year, income_band, effective_rate_band)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE MATERIALIZED VIEW gold_tax_income_bands AS
SELECT
    tax_year,

    -- ── Income band ───────────────────────────────────────────────────────────
    CASE
        WHEN gross_income <  25000                    THEN '1_Under $25K'
        WHEN gross_income BETWEEN  25000 AND  50000   THEN '2_$25K–$50K'
        WHEN gross_income BETWEEN  50001 AND 100000   THEN '3_$50K–$100K'
        WHEN gross_income BETWEEN 100001 AND 200000   THEN '4_$100K–$200K'
        WHEN gross_income BETWEEN 200001 AND 500000   THEN '5_$200K–$500K'
        ELSE                                               '6_Over $500K'
    END AS income_band,

    -- ── Effective tax rate band ───────────────────────────────────────────────
    CASE
        WHEN effective_tax_rate <  5   THEN '1_<5%'
        WHEN effective_tax_rate < 10   THEN '2_5–10%'
        WHEN effective_tax_rate < 15   THEN '3_10–15%'
        WHEN effective_tax_rate < 20   THEN '4_15–20%'
        WHEN effective_tax_rate < 30   THEN '5_20–30%'
        ELSE                                '6_30%+'
    END AS effective_rate_band,

    -- ── Aggregates ────────────────────────────────────────────────────────────
    COUNT(taxpayer_id)                AS taxpayer_count,
    ROUND(AVG(gross_income),    2)    AS avg_gross_income,
    ROUND(AVG(taxable_income),  2)    AS avg_taxable_income,
    ROUND(AVG(deductions),      2)    AS avg_deductions,
    ROUND(AVG(tax_liability),   2)    AS avg_tax_liability,
    ROUND(SUM(tax_liability),   2)    AS total_tax_liability,
    ROUND(AVG(effective_tax_rate), 2) AS avg_effective_tax_rate,
    ROUND(SUM(balance_due),     2)    AS total_balance_due,
    ROUND(SUM(refund_amount),   2)    AS total_refund_amount

FROM silver_tax_records
GROUP BY
    tax_year,
    CASE
        WHEN gross_income <  25000                    THEN '1_Under $25K'
        WHEN gross_income BETWEEN  25000 AND  50000   THEN '2_$25K–$50K'
        WHEN gross_income BETWEEN  50001 AND 100000   THEN '3_$50K–$100K'
        WHEN gross_income BETWEEN 100001 AND 200000   THEN '4_$100K–$200K'
        WHEN gross_income BETWEEN 200001 AND 500000   THEN '5_$200K–$500K'
        ELSE                                               '6_Over $500K'
    END,
    CASE
        WHEN effective_tax_rate <  5   THEN '1_<5%'
        WHEN effective_tax_rate < 10   THEN '2_5–10%'
        WHEN effective_tax_rate < 15   THEN '3_10–15%'
        WHEN effective_tax_rate < 20   THEN '4_15–20%'
        WHEN effective_tax_rate < 30   THEN '5_20–30%'
        ELSE                                '6_30%+'
    END
ORDER BY
    tax_year DESC,
    income_band,
    effective_rate_band
