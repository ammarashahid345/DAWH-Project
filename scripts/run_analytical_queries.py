import subprocess
import time

QUERIES = {
    "Hospital_Forecast_Consumption": """
        SELECT
            h.hospital_name,
            d.year_month,
            SUM(f.forecast_demand_packs) AS forecast_packs,
            SUM(f.consumed_quantity_packs) AS actual_consumption,
            SUM(f.unfulfilled_demand_packs) AS unfulfilled_demand,
            ROUND(
                100.0 * SUM(f.consumed_quantity_packs) /
                NULLIF(SUM(f.forecast_demand_packs), 0), 2
            ) AS consumption_vs_forecast_pct
        FROM mart.fact_demand f
        JOIN mart.dim_hospital h ON f.hospital_pk = h.hospital_pk
        JOIN mart.dim_date d ON f.date_pk = d.date_pk
        GROUP BY h.hospital_name, d.year_month
        ORDER BY h.hospital_name, d.year_month;
    """,

    "Sales_Channel_Performance": """
        SELECT
            CASE
                WHEN f.sales_channel = 'Ecommerce'
                    THEN 'E-commerce'
                WHEN p.pharmacy_type = 'Hospital Pharmacy'
                    THEN 'Hospital Pharmacy'
                ELSE 'Retail Pharmacy'
            END AS sales_channel,
            SUM(f.quantity_sold_packs) AS quantity_sold,
            ROUND(SUM(f.net_sales_amount_usd), 2) AS net_sales_usd,
            ROUND(SUM(f.gross_margin_usd), 2) AS gross_margin_usd,
            ROUND(
                100.0 * SUM(f.gross_margin_usd) /
                NULLIF(SUM(f.net_sales_amount_usd), 0), 2
            ) AS gross_margin_pct
        FROM mart.fact_sales f
        JOIN mart.dim_pharmacy p ON f.pharmacy_pk = p.pharmacy_pk
        GROUP BY 1
        ORDER BY net_sales_usd DESC;
    """,

    "Top_Fast_Slow_Medicines": """
        WITH medicine_sales AS (
            SELECT
                m.therapeutic_class,
                m.medicine_name,
                SUM(f.quantity_sold_packs) AS total_quantity_sold
            FROM mart.fact_sales f
            JOIN mart.dim_medicine m ON f.med_pk = m.med_pk
            WHERE m.cold_chain = 'Yes'
            GROUP BY m.therapeutic_class, m.medicine_name
        ),
        ranked AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    ORDER BY total_quantity_sold DESC
                ) AS fast_rank,
                ROW_NUMBER() OVER (
                    ORDER BY total_quantity_sold ASC
                ) AS slow_rank
            FROM medicine_sales
        )
        SELECT
            therapeutic_class,
            medicine_name,
            total_quantity_sold,
            CASE
                WHEN fast_rank <= 10 THEN 'Fast-moving'
                WHEN slow_rank <= 10 THEN 'Slow-moving'
            END AS movement_type
        FROM ranked
        WHERE fast_rank <= 10 OR slow_rank <= 10
        ORDER BY movement_type, total_quantity_sold DESC;
    """
}

for name, query in QUERIES.items():
    print("\\n" + "=" * 65)
    print("QUERY:", name)

    start = time.perf_counter()
    status = "Success"
    error = None

    try:
        result = subprocess.run(
            [
                "docker", "compose", "exec", "-T", "postgres",
                "psql", "-X", "-P", "pager=off",
                "-v", "ON_ERROR_STOP=1",
                "-U", "airflow", "-d", "pharma_dwh",
                "-c", query
            ],
            capture_output=True,
            text=True,
            check=True
        )
        print(result.stdout)

    except subprocess.CalledProcessError as exc:
        status = "Failed"
        error = (exc.stderr or str(exc))[:4000]
        print("ERROR:", error)

    elapsed_ms = round((time.perf_counter() - start) * 1000, 2)

    import_sql = (
        "INSERT INTO analytics.query_execution_log "
        "(query_name, execution_time_ms, query_status, error_message) "
        "VALUES ("
        + "'" + name.replace("'", "''") + "', "
        + str(elapsed_ms) + ", "
        + "'" + status + "', "
        + ("NULL" if error is None else "'" + error.replace("'", "''") + "'")
        + ");"
    )

    subprocess.run(
        [
            "docker", "compose", "exec", "-T", "postgres",
            "psql", "-X", "-v", "ON_ERROR_STOP=1",
            "-U", "airflow", "-d", "pharma_dwh",
            "-c", import_sql
        ],
        check=True
    )

    print("STATUS:", status)
    print("EXECUTION TIME:", elapsed_ms, "ms")
    print("LOGGED IN DATABASE")

print("\\nAll analytical queries completed.")
