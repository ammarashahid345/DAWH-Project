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


def validate_demand_data():
    conn = get_connection()
    try:
        cur = conn.cursor()

        cur.execute("""
            SELECT
                COUNT(*) AS source_rows,
                COUNT(*) FILTER (
                    WHERE h.hospital_pk IS NULL
                       OR m.med_pk IS NULL
                       OR p.pharmacy_pk IS NULL
                       OR d.date_pk IS NULL
                ) AS unmatched_rows,
                COUNT(*) - COUNT(DISTINCT r."HospitalDemandRecordID")
                    AS duplicate_rows
            FROM raw.hospital_demand_data r
            LEFT JOIN mart.dim_hospital h
                ON h.hospital_id = r."HospitalID"
            LEFT JOIN mart.dim_medicine m
                ON m.medicine_id = r."MedicineID"
            LEFT JOIN mart.dim_pharmacy p
                ON p.pharmacy_id = r."HospitalPharmacyID"
            LEFT JOIN mart.dim_date d
                ON d.full_date = TO_DATE(r."DemandMonth", 'DD-MM-YYYY');
        """)

        source_rows, unmatched_rows, duplicate_rows = cur.fetchone()

        if source_rows == 0:
            raise ValueError("No hospital demand records found.")

        if unmatched_rows > 0:
            raise ValueError(
                f"{unmatched_rows} records have unmatched dimension keys."
            )

        if duplicate_rows > 0:
            raise ValueError(
                f"{duplicate_rows} duplicate source record IDs found."
            )

        print(f"Source records validated: {source_rows}")
        print("Unmatched dimension records: 0")
        print("Duplicate source records: 0")

        cur.close()
    finally:
        conn.close()


def load_fact_demand():
    conn = get_connection()
    try:
        cur = conn.cursor()

        cur.execute("""
            INSERT INTO mart.fact_demand (
                hospital_demand_record_id,
                date_pk,
                hospital_pk,
                med_pk,
                pharmacy_pk,
                estimated_disease_cases,
                disease_related_admissions,
                outpatient_encounters,
                forecast_demand_packs,
                requested_quantity_packs,
                approved_quantity_packs,
                received_quantity_packs,
                emergency_request_quantity_packs,
                opening_stock_packs,
                dispensed_quantity_packs,
                consumed_quantity_packs,
                wastage_quantity_packs,
                closing_stock_packs,
                unfulfilled_demand_packs,
                stockout_days,
                service_level_pct,
                average_lead_time_days,
                demand_trend_index,
                medication_request_status,
                formulary_status
            )
            SELECT
                r."HospitalDemandRecordID",
                d.date_pk,
                h.hospital_pk,
                m.med_pk,
                p.pharmacy_pk,
                NULLIF(TRIM(r."EstimatedDiseaseCases"), '')::INTEGER,
                NULLIF(TRIM(r."DiseaseRelatedAdmissions"), '')::INTEGER,
                NULLIF(TRIM(r."OutpatientEncounters"), '')::INTEGER,
                NULLIF(TRIM(r."ForecastDemandPacks"), '')::INTEGER,
                NULLIF(TRIM(r."RequestedQuantityPacks"), '')::INTEGER,
                NULLIF(TRIM(r."ApprovedQuantityPacks"), '')::INTEGER,
                NULLIF(TRIM(r."ReceivedQuantityPacks"), '')::INTEGER,
                NULLIF(TRIM(r."EmergencyRequestQuantityPacks"), '')::INTEGER,
                NULLIF(TRIM(r."OpeningStockPacks"), '')::INTEGER,
                NULLIF(TRIM(r."DispensedQuantityPacks"), '')::INTEGER,
                NULLIF(TRIM(r."ConsumedQuantityPacks"), '')::INTEGER,
                NULLIF(TRIM(r."WastageQuantityPacks"), '')::INTEGER,
                NULLIF(TRIM(r."ClosingStockPacks"), '')::INTEGER,
                NULLIF(TRIM(r."UnfulfilledDemandPacks"), '')::INTEGER,
                NULLIF(TRIM(r."StockoutDays"), '')::INTEGER,
                NULLIF(TRIM(r."ServiceLevelPct"), '')::NUMERIC,
                NULLIF(TRIM(r."AverageLeadTimeDays"), '')::NUMERIC,
                NULLIF(TRIM(r."DemandTrendIndex"), '')::NUMERIC,
                NULLIF(TRIM(r."MedicationRequestStatus"), ''),
                NULLIF(TRIM(r."FormularyStatus"), '')
            FROM raw.hospital_demand_data r
            JOIN mart.dim_hospital h
                ON h.hospital_id = r."HospitalID"
            JOIN mart.dim_medicine m
                ON m.medicine_id = r."MedicineID"
            JOIN mart.dim_pharmacy p
                ON p.pharmacy_id = r."HospitalPharmacyID"
            JOIN mart.dim_date d
                ON d.full_date = TO_DATE(r."DemandMonth", 'DD-MM-YYYY')
            ON CONFLICT (hospital_demand_record_id)
            DO UPDATE SET
                date_pk = EXCLUDED.date_pk,
                hospital_pk = EXCLUDED.hospital_pk,
                med_pk = EXCLUDED.med_pk,
                pharmacy_pk = EXCLUDED.pharmacy_pk,
                estimated_disease_cases = EXCLUDED.estimated_disease_cases,
                disease_related_admissions = EXCLUDED.disease_related_admissions,
                outpatient_encounters = EXCLUDED.outpatient_encounters,
                forecast_demand_packs = EXCLUDED.forecast_demand_packs,
                requested_quantity_packs = EXCLUDED.requested_quantity_packs,
                approved_quantity_packs = EXCLUDED.approved_quantity_packs,
                received_quantity_packs = EXCLUDED.received_quantity_packs,
                emergency_request_quantity_packs = EXCLUDED.emergency_request_quantity_packs,
                opening_stock_packs = EXCLUDED.opening_stock_packs,
                dispensed_quantity_packs = EXCLUDED.dispensed_quantity_packs,
                consumed_quantity_packs = EXCLUDED.consumed_quantity_packs,
                wastage_quantity_packs = EXCLUDED.wastage_quantity_packs,
                closing_stock_packs = EXCLUDED.closing_stock_packs,
                unfulfilled_demand_packs = EXCLUDED.unfulfilled_demand_packs,
                stockout_days = EXCLUDED.stockout_days,
                service_level_pct = EXCLUDED.service_level_pct,
                average_lead_time_days = EXCLUDED.average_lead_time_days,
                demand_trend_index = EXCLUDED.demand_trend_index,
                medication_request_status = EXCLUDED.medication_request_status,
                formulary_status = EXCLUDED.formulary_status;
        """)

        print(f"Fact demand rows inserted/updated: {cur.rowcount}")

        conn.commit()
        cur.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def verify_fact_demand():
    conn = get_connection()
    try:
        cur = conn.cursor()

        cur.execute("""
            SELECT
                COUNT(*) AS total_records,
                COUNT(DISTINCT hospital_demand_record_id) AS unique_records,
                COUNT(DISTINCT d.year_month) AS total_months,
                MIN(d.full_date) AS first_month,
                MAX(d.full_date) AS last_month
            FROM mart.fact_demand f
            JOIN mart.dim_date d
                ON d.date_pk = f.date_pk;
        """)

        result = cur.fetchone()

        print(f"Fact table records: {result[0]}")
        print(f"Unique source records: {result[1]}")
        print(f"Distinct year-months: {result[2]}")
        print(f"First month: {result[3]}")
        print(f"Last month: {result[4]}")

        if result[0] != result[1]:
            raise ValueError("Duplicate source records detected in fact table.")

        if result[2] == 0:
            raise ValueError("No valid date records found in fact table.")

        cur.close()
    finally:
        conn.close()


with DAG(
    dag_id="build_demand_mart",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["pharma", "demand", "transformation"],
) as dag:

    validate = PythonOperator(
        task_id="validate_demand_data",
        python_callable=validate_demand_data,
    )

    load = PythonOperator(
        task_id="load_fact_demand",
        python_callable=load_fact_demand,
    )

    verify = PythonOperator(
        task_id="verify_fact_demand",
        python_callable=verify_fact_demand,
    )

    validate >> load >> verify
