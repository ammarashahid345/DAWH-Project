-- Query 1: Hospital-wise Monthly Demand Analysis
-- Purpose: Compare forecast demand, actual consumption,
-- unfulfilled demand and demand variance for each hospital and month.

SELECT
    h.hospital_id,
    h.hospital_name,
    d.year_month,
    SUM(f.forecast_demand_packs) AS forecast_demand_packs,
    SUM(f.consumed_quantity_packs) AS actual_consumption_packs,
    SUM(f.unfulfilled_demand_packs) AS unfulfilled_demand_packs,
    SUM(f.consumed_quantity_packs) - SUM(f.forecast_demand_packs)
        AS demand_variance_packs,
    ROUND(
        100.0 * SUM(f.consumed_quantity_packs)
        / NULLIF(SUM(f.forecast_demand_packs), 0), 2
    ) AS consumption_vs_forecast_pct
FROM mart.fact_demand f
JOIN mart.dim_hospital h
    ON f.hospital_pk = h.hospital_pk
JOIN mart.dim_date d
    ON f.date_pk = d.date_pk
GROUP BY
    h.hospital_id,
    h.hospital_name,
    d.year_month
ORDER BY
    d.year_month,
    h.hospital_name;
