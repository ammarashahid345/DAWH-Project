import csv
import os
import time
from datetime import datetime

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

from airflow import DAG
from airflow.decorators import task


DB_CONFIG = {
    "host": "postgres",
    "port": 5432,
    "dbname": "pharma_dwh",
    "user": "airflow",
    "password": "airflow",
}

DATA_DIR = "/opt/airflow/data"

# Load the three original source files not yet present in raw.
LOADS = [
    ("raw/02_manufacturing_data.csv", "manufacturing_data",
     ["ManufacturingOrderID"]),

    ("raw/03_manufacturer_delivery_data.csv", "manufacturer_delivery_data",
     ["ShipmentID", "ShipmentLineNumber"]),

    ("raw/07_iot_streaming_data.csv", "iot_streaming_data",
     ["SensorReadingID"]),
]

# Then load all seven files for each synthetic refresh.
SOURCE_FILES = [
    ("01_master_data.csv", "master_data", ["MedicineID"]),
    ("02_manufacturing_data.csv", "manufacturing_data", ["ManufacturingOrderID"]),
    ("03_manufacturer_delivery_data.csv", "manufacturer_delivery_data",
     ["ShipmentID", "ShipmentLineNumber"]),
    ("04_warehouse_data.csv", "warehouse_data", ["InventorySnapshotID"]),
    ("05_pharmacy_sales_data.csv", "pharmacy_sales_data", ["SalesTransactionID"]),
    ("06_hospital_demand_data.csv", "hospital_demand_data",
     ["HospitalDemandRecordID"]),
    ("07_iot_streaming_data.csv", "iot_streaming_data", ["SensorReadingID"]),
]

for refresh in ["refresh_1", "refresh_2"]:
    for filename, table, primary_key in SOURCE_FILES:
        LOADS.append((
            f"synthetic_staging/{refresh}/{filename}",
            table,
            primary_key,
        ))


def write_batch_log(conn, source_file, start_time, end_time,
                    duration, rows, status, error=None):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO analytics.batch_ingestion_log
            (source_file, start_time, end_time, duration_seconds,
             rows_processed, batch_status, error_message)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (source_file, start_time, end_time, duration,
             rows, status, error),
        )
    conn.commit()


@task
def ingest_file(relative_path, table_name, primary_key):
    start_time = datetime.now()
    timer = time.time()
    rows_processed = 0
    conn = psycopg2.connect(**DB_CONFIG)

    try:
        file_path = os.path.join(DATA_DIR, relative_path)

        with open(file_path, "r", encoding="utf-8-sig", newline="") as file:
            reader = csv.DictReader(file)
            headers = reader.fieldnames

            if not headers:
                raise ValueError(f"No CSV headers found: {relative_path}")

            missing_keys = [key for key in primary_key if key not in headers]
            if missing_keys:
                raise ValueError(f"Missing primary key columns: {missing_keys}")

            with conn.cursor() as cur:
                column_definitions = sql.SQL(", ").join(
                    sql.SQL("{} TEXT").format(sql.Identifier(column))
                    for column in headers
                )

                primary_key_sql = sql.SQL(", ").join(
                    sql.Identifier(key) for key in primary_key
                )

                cur.execute(
                    sql.SQL(
                        "CREATE TABLE IF NOT EXISTS raw.{} "
                        "({}, PRIMARY KEY ({}))"
                    ).format(
                        sql.Identifier(table_name),
                        column_definitions,
                        primary_key_sql,
                    )
                )

                insert_query = sql.SQL(
                    "INSERT INTO raw.{} ({}) VALUES %s ON CONFLICT DO NOTHING"
                ).format(
                    sql.Identifier(table_name),
                    sql.SQL(", ").join(
                        sql.Identifier(column) for column in headers
                    ),
                )

                batch = []

                for row in reader:
                    batch.append(tuple(row[column] for column in headers))

                    if len(batch) >= 1000:
                        execute_values(
                            cur, insert_query.as_string(conn),
                            batch, page_size=1000
                        )
                        rows_processed += len(batch)
                        batch.clear()

                if batch:
                    execute_values(
                        cur, insert_query.as_string(conn),
                        batch, page_size=1000
                    )
                    rows_processed += len(batch)

        conn.commit()

        end_time = datetime.now()
        duration = round(time.time() - timer, 2)

        write_batch_log(
            conn, relative_path, start_time, end_time,
            duration, rows_processed, "Success"
        )

        print(
            f"SUCCESS: {relative_path} | "
            f"Rows read: {rows_processed} | "
            f"Duration: {duration}s"
        )

    except Exception as error:
        conn.rollback()
        end_time = datetime.now()
        duration = round(time.time() - timer, 2)

        write_batch_log(
            conn, relative_path, start_time, end_time,
            duration, rows_processed, "Failed", str(error)
        )

        raise

    finally:
        conn.close()


with DAG(
    dag_id="ingest_synthetic_refresh",
    description="Load missing original sources and two synthetic refresh cycles",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 1},
    tags=["pharma", "synthetic-data", "ingestion"],
) as dag:

    ingestion_tasks = []

    for relative_path, table_name, primary_key in LOADS:
        task_name = (
            "load_" + relative_path.replace("/", "_").replace(".csv", "")
        )

        current_task = ingest_file.override(
            task_id=task_name
        )(relative_path, table_name, primary_key)

        ingestion_tasks.append(current_task)

    for previous_task, next_task in zip(
        ingestion_tasks, ingestion_tasks[1:]
    ):
        previous_task >> next_task
