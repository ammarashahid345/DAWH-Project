from datetime import datetime

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator


DB_CONFIG = {
    "host": "postgres",
    "port": 5432,
    "dbname": "pharma_dwh",
    "user": "airflow",
    "password": "airflow",
}


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def validate_sales_data():
    conn = get_connection()
    try:
        cur = conn.cursor()

        cur.execute("""
            SELECT
                COUNT(*) AS source_rows,

                COUNT(*) FILTER (
                    WHERE m.med_pk IS NULL
                       OR p.pharmacy_pk IS NULL
                       OR d.date_pk IS NULL
                       OR (
                            NULLIF(TRIM(r."SourceWarehouseID"), '') IS NOT NULL
                            AND w.warehouse_pk IS NULL
                       )
                       OR (
                            NULLIF(TRIM(r."HospitalID"), '') IS NOT NULL
                            AND h.hospital_pk IS NULL
                       )
                       OR (
                            NULLIF(TRIM(r."PharmacyDeliveryDate"), '') IS NOT NULL
                            AND dd.date_pk IS NULL
                       )
                ) AS unmatched_rows,

                COUNT(*) - COUNT(DISTINCT r."SalesTransactionID")
                    AS duplicate_rows,

                COUNT(*) FILTER (
                    WHERE NULLIF(TRIM(r."SalesDate"), '') IS NULL
                       OR NULLIF(TRIM(r."MedicineID"), '') IS NULL
                       OR NULLIF(TRIM(r."PharmacyID"), '') IS NULL
                ) AS missing_required_rows

            FROM raw.pharmacy_sales_data r

            LEFT JOIN mart.dim_medicine m
                ON m.medicine_id = r."MedicineID"

            LEFT JOIN mart.dim_pharmacy p
                ON p.pharmacy_id = r."PharmacyID"

            LEFT JOIN mart.dim_warehouse w
                ON w.warehouse_id = r."SourceWarehouseID"

            LEFT JOIN mart.dim_hospital h
                ON h.hospital_id = NULLIF(TRIM(r."HospitalID"), '')

            LEFT JOIN mart.dim_date d
                ON d.full_date =
                   DATE '1899-12-30'
                   + FLOOR(r."SalesDate"::numeric)::integer

            LEFT JOIN mart.dim_date dd
                ON dd.full_date =
                   DATE '1899-12-30'
                   + FLOOR(NULLIF(TRIM(r."PharmacyDeliveryDate"), '')::numeric)::integer;
        """)

        source_rows, unmatched_rows, duplicate_rows, missing_rows = cur.fetchone()

        if source_rows == 0:
            raise ValueError("No pharmacy sales records found.")

        if unmatched_rows > 0:
            raise ValueError(
                f"{unmatched_rows} records have unmatched dimension/date keys."
            )

        if duplicate_rows > 0:
            raise ValueError(
                f"{duplicate_rows} duplicate sales transaction IDs found."
            )

        if missing_rows > 0:
            raise ValueError(
                f"{missing_rows} records have missing required fields."
            )

        print(f"Source sales records validated: {source_rows}")
        print("Unmatched dimension/date records: 0")
        print("Duplicate transaction IDs: 0")
        print("Missing required fields: 0")

        cur.close()
    finally:
        conn.close()


def load_fact_sales():
    conn = get_connection()
    try:
        cur = conn.cursor()

        cur.execute("""
            INSERT INTO mart.fact_sales (
                sales_transaction_id,
                date_pk,
                med_pk,
                pharmacy_pk,
                delivery_date_pk,
                sales_timestamp_utc,
                sales_channel,
                delivery_quantity_packs,
                delivery_otif_flag,
                quantity_sold_packs,
                unit_retail_price_usd,
                gross_sales_amount_usd,
                discount_amount_usd,
                net_sales_amount_usd,
                sales_tax_usd,
                cost_of_goods_sold_usd,
                gross_margin_usd,
                promotion_code,
                prescription_required_flag,
                customer_segment,
                payer_type,
                payment_method,
                ecommerce_delivery_status,
                return_flag,
                returned_quantity_packs,
                sale_status,
                warehouse_pk,
                hospital_pk
            )

            SELECT
                r."SalesTransactionID",
                d.date_pk,
                m.med_pk,
                p.pharmacy_pk,
                dd.date_pk,

                (
                    TIMESTAMP '1899-12-30'
                    + r."SalesTimestampUTC"::numeric * INTERVAL '1 day'
                ) AT TIME ZONE 'UTC',

                NULLIF(TRIM(r."SalesChannel"), ''),
                NULLIF(TRIM(r."DeliveryQuantityPacks"), '')::integer,

                CASE UPPER(TRIM(r."DeliveryOTIFFlag"))
                    WHEN 'Y' THEN TRUE
                    WHEN 'N' THEN FALSE
                    ELSE NULL
                END,

                NULLIF(TRIM(r."QuantitySoldPacks"), '')::integer,
                NULLIF(TRIM(r."UnitRetailPriceUSD"), '')::numeric,
                NULLIF(TRIM(r."GrossSalesAmountUSD"), '')::numeric,
                NULLIF(TRIM(r."DiscountAmountUSD"), '')::numeric,
                NULLIF(TRIM(r."NetSalesAmountUSD"), '')::numeric,
                NULLIF(TRIM(r."SalesTaxUSD"), '')::numeric,
                NULLIF(TRIM(r."CostOfGoodsSoldUSD"), '')::numeric,
                NULLIF(TRIM(r."GrossMarginUSD"), '')::numeric,

                NULLIF(TRIM(r."PromotionCode"), ''),

                CASE UPPER(TRIM(r."PrescriptionRequiredFlag"))
                    WHEN 'Y' THEN TRUE
                    WHEN 'N' THEN FALSE
                    ELSE NULL
                END,

                NULLIF(TRIM(r."CustomerSegment"), ''),
                NULLIF(TRIM(r."PayerType"), ''),
                NULLIF(TRIM(r."PaymentMethod"), ''),
                NULLIF(TRIM(r."EcommerceDeliveryStatus"), ''),

                CASE UPPER(TRIM(r."ReturnFlag"))
                    WHEN 'Y' THEN TRUE
                    WHEN 'N' THEN FALSE
                    ELSE NULL
                END,

                NULLIF(TRIM(r."ReturnedQuantityPacks"), '')::integer,
                NULLIF(TRIM(r."SaleStatus"), ''),
                w.warehouse_pk,
                h.hospital_pk

            FROM raw.pharmacy_sales_data r

            JOIN mart.dim_medicine m
                ON m.medicine_id = r."MedicineID"

            JOIN mart.dim_pharmacy p
                ON p.pharmacy_id = r."PharmacyID"

            JOIN mart.dim_date d
                ON d.full_date =
                   DATE '1899-12-30'
                   + FLOOR(r."SalesDate"::numeric)::integer

            LEFT JOIN mart.dim_date dd
                ON dd.full_date =
                   DATE '1899-12-30'
                   + FLOOR(NULLIF(TRIM(r."PharmacyDeliveryDate"), '')::numeric)::integer

            LEFT JOIN mart.dim_warehouse w
                ON w.warehouse_id = NULLIF(TRIM(r."SourceWarehouseID"), '')

            LEFT JOIN mart.dim_hospital h
                ON h.hospital_id = NULLIF(TRIM(r."HospitalID"), '')

            ON CONFLICT (sales_transaction_id)
            DO UPDATE SET
                date_pk = EXCLUDED.date_pk,
                med_pk = EXCLUDED.med_pk,
                pharmacy_pk = EXCLUDED.pharmacy_pk,
                delivery_date_pk = EXCLUDED.delivery_date_pk,
                sales_timestamp_utc = EXCLUDED.sales_timestamp_utc,
                sales_channel = EXCLUDED.sales_channel,
                delivery_quantity_packs = EXCLUDED.delivery_quantity_packs,
                delivery_otif_flag = EXCLUDED.delivery_otif_flag,
                quantity_sold_packs = EXCLUDED.quantity_sold_packs,
                unit_retail_price_usd = EXCLUDED.unit_retail_price_usd,
                gross_sales_amount_usd = EXCLUDED.gross_sales_amount_usd,
                discount_amount_usd = EXCLUDED.discount_amount_usd,
                net_sales_amount_usd = EXCLUDED.net_sales_amount_usd,
                sales_tax_usd = EXCLUDED.sales_tax_usd,
                cost_of_goods_sold_usd = EXCLUDED.cost_of_goods_sold_usd,
                gross_margin_usd = EXCLUDED.gross_margin_usd,
                promotion_code = EXCLUDED.promotion_code,
                prescription_required_flag = EXCLUDED.prescription_required_flag,
                customer_segment = EXCLUDED.customer_segment,
                payer_type = EXCLUDED.payer_type,
                payment_method = EXCLUDED.payment_method,
                ecommerce_delivery_status = EXCLUDED.ecommerce_delivery_status,
                return_flag = EXCLUDED.return_flag,
                returned_quantity_packs = EXCLUDED.returned_quantity_packs,
                sale_status = EXCLUDED.sale_status,
                warehouse_pk = EXCLUDED.warehouse_pk,
                hospital_pk = EXCLUDED.hospital_pk;
        """)

        print(f"Fact sales rows inserted/updated: {cur.rowcount}")

        conn.commit()
        cur.close()

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def verify_fact_sales():
    conn = get_connection()
    try:
        cur = conn.cursor()

        cur.execute("""
            SELECT
                COUNT(*) AS total_records,
                COUNT(DISTINCT sales_transaction_id) AS unique_records,
                COUNT(DISTINCT d.year_month) AS total_months,
                MIN(d.full_date) AS first_sale_date,
                MAX(d.full_date) AS last_sale_date,
                SUM(f.net_sales_amount_usd) AS total_net_sales,
                SUM(f.gross_margin_usd) AS total_gross_margin
            FROM mart.fact_sales f
            JOIN mart.dim_date d
                ON d.date_pk = f.date_pk;
        """)

        result = cur.fetchone()

        print(f"Fact sales records: {result[0]}")
        print(f"Unique transaction IDs: {result[1]}")
        print(f"Distinct year-months: {result[2]}")
        print(f"First sales date: {result[3]}")
        print(f"Last sales date: {result[4]}")
        print(f"Total net sales USD: {result[5]}")
        print(f"Total gross margin USD: {result[6]}")

        if result[0] != result[1]:
            raise ValueError("Duplicate transaction IDs found in fact_sales.")

        if result[0] == 0:
            raise ValueError("Fact sales table is empty.")

        cur.close()
    finally:
        conn.close()


with DAG(
    dag_id="build_sales_mart",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["pharma", "sales", "transformation"],
) as dag:

    validate = PythonOperator(
        task_id="validate_sales_data",
        python_callable=validate_sales_data,
    )

    load = PythonOperator(
        task_id="load_fact_sales",
        python_callable=load_fact_sales,
    )

    verify = PythonOperator(
        task_id="verify_fact_sales",
        python_callable=verify_fact_sales,
    )

    validate >> load >> verify
