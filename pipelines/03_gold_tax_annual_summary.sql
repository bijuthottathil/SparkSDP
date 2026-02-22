-- ─────────────────────────────────────────────────────────────────────────────
-- Gold Layer 1 — Annual Tax Summary by State & Filing Status
--
-- Business use: tax revenue reporting, state-level dashboards, YoY trending.
-- Grain: (tax_year, state, filing_status)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE MATERIALIZED VIEW gold_tax_annual_summary AS
SELECT
    tax_year,
    state,
    filing_status,

    -- ── Volume ────────────────────────────────────────────────────────────────
    COUNT(taxpayer_id)                                      AS taxpayer_count,

    -- ── Income ───────────────────────────────────────────────────────────────
    ROUND(SUM(gross_income),   2)                           AS total_gross_income,
    ROUND(AVG(gross_income),   2)                           AS avg_gross_income,
    ROUND(SUM(taxable_income), 2)                           AS total_taxable_income,
    ROUND(AVG(taxable_income), 2)                           AS avg_taxable_income,

    -- ── Tax liability ─────────────────────────────────────────────────────────
    ROUND(SUM(tax_liability),           2)                  AS total_tax_liability,
    ROUND(AVG(tax_liability),           2)                  AS avg_tax_liability,
    ROUND(AVG(effective_tax_rate),      2)                  AS avg_effective_tax_rate,

    -- ── Payments & balance ────────────────────────────────────────────────────
    ROUND(SUM(tax_paid),    2)                              AS total_tax_paid,
    ROUND(SUM(balance_due), 2)                              AS total_balance_due,
    ROUND(SUM(refund_amount), 2)                            AS total_refund_amount,

    -- ── Compliance flags ──────────────────────────────────────────────────────
    COUNT(CASE WHEN balance_due   > 0 THEN 1 END)           AS taxpayers_with_balance_due,
    COUNT(CASE WHEN refund_amount > 0 THEN 1 END)           AS taxpayers_receiving_refund,
    ROUND(
        COUNT(CASE WHEN balance_due > 0 THEN 1 END) * 100.0 / COUNT(taxpayer_id),
        2
    )                                                       AS pct_with_balance_due

FROM silver_tax_records
GROUP BY
    tax_year,
    state,
    filing_status
ORDER BY
    tax_year        DESC,
    total_tax_liability DESC
