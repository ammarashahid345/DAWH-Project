-- Query 2: Forecast Accuracy and Hospital Request Fulfilment
-- Purpose: Analyse forecast accuracy and request fulfilment
-- by year, hospital type and disease category.

SELECT
    d.year,
    h.hospital_type,
    m.disease_category,
    SUM(f.forecast_demand_packs) AS total_forecast_packs,
    SUM(f.consumed_quantity_packs) AS actual_consumption_packs,
    ROUND(
        100.0 * (
            1 - SUM(ABS(
                f.consumed_quantity_packs - f.forecast_demand_packs
            ))::numeric
            / NULLIF(SUM(f.consumed_quantity_packs), 0)
        ), 2
    ) AS forecast_accuracy_pct,
    SUM(f.requested_quantity_packs) AS total_requested_packs,
    SUM(f.received_quantity_packs) AS total_received_packs,
    ROUND(
        100.0 * SUM(f.received_quantity_packs)
        / NULLIF(SUM(f.requested_quantity_packs), 0), 2
    ) AS request_fulfilment_rate_pct
FROM mart.fact_demand f
JOIN mart.dim_date d
    ON f.date_pk = d.date_pk
JOIN mart.dim_hospital h
    ON f.hospital_pk = h.hospital_pk
JOIN mart.dim_medicine m
    ON f.med_pk = m.med_pk
GROUP BY
    d.year,
    h.hospital_type,
    m.disease_category
ORDER BY
    d.year,
    h.hospital_type,
    m.disease_category;
