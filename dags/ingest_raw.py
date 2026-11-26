import csv
import os
import time
from datetime import datetime

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

from airflow.sdk import DAG, task


DB_CONFIG = {
    "host": "postgres",
    "port": 5432,
    "dbname": "pharma_dwh",
    "user": "airflow",
    "password": "airflow"
}


FILES = [
    {
        "file": "/opt/airflow/data/raw/01_master_data.csv",
        "table": "master_data",
        "pk": "MedicineID"
    },
    {
        "file": "/opt/airflow/data/raw/04_warehouse_data.csv",
        "table": "warehouse_data",
        "pk": "InventorySnapshotID"
    },
    {
        "file": "/opt/airflow/data/raw/05_pharmacy_sales_data.csv",
        "table": "pharmacy_sales_data",
        "pk": "SalesTransactionID"
    },
    {
        "file": "/opt/airflow/data/raw/06_hospital_demand_data.csv",
        "table": "hospital_demand_data",
        "pk": "HospitalDemandRecordID"
    }
]


def record_batch_log(source_file, start_time, duration,
                     rows_processed, status, error_message=None):

    conn = psycopg2.connect(**DB_CONFIG)

    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO analytics.batch_ingestion_log
                (
                    source_file,
                    start_time,
                    end_time,
                    duration_seconds,
                    rows_processed,
                    batch_status,
                    error_message
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                source_file,
                start_time,
                datetime.now(),
                round(duration, 2),
                rows_processed,
                status,
                error_message
            ))

        conn.commit()

    finally:
        conn.close()


with DAG(
    dag_id="ingest_raw",
    start_date=datetime(2026, 11, 22),
    schedule=None,
    catchup=False,
    tags=["pharma", "ingestion"]
) as dag:

    @task
    def load_csv(file_path, table_name, primary_key):

        source_file = os.path.basename(file_path)
        start_time = datetime.now()
        start_clock = time.perf_counter()
        total_rows = 0

        print(f"Loading {source_file}")

        conn = psycopg2.connect(**DB_CONFIG)

        try:
            with open(
                file_path,
                "r",
                encoding="utf-8-sig",
                newline=""
            ) as file:

                reader = csv.DictReader(file)
                columns = reader.fieldnames

                if not columns:
                    raise ValueError(f"No columns found in {file_path}")

                if primary_key not in columns:
                    raise ValueError(
                        f"Primary key {primary_key} not found"
                    )

                column_definitions = [
                    sql.SQL("{} TEXT").format(sql.Identifier(column))
                    for column in columns
                ]

                create_query = sql.SQL(
                    "CREATE TABLE IF NOT EXISTS raw.{} ({}, PRIMARY KEY ({}))"
                ).format(
                    sql.Identifier(table_name),
                    sql.SQL(", ").join(column_definitions),
                    sql.Identifier(primary_key)
                )

                with conn.cursor() as cursor:

                    cursor.execute(create_query)

                    insert_columns = sql.SQL(", ").join(
                        sql.Identifier(column) for column in columns
                    )

                    update_columns = [
                        column for column in columns
                        if column != primary_key
                    ]

                    update_clause = sql.SQL(", ").join(
                        sql.SQL("{} = EXCLUDED.{}").format(
                            sql.Identifier(column),
                            sql.Identifier(column)
                        )
                        for column in update_columns
                    )

                    insert_query = sql.SQL("""
                        INSERT INTO raw.{} ({})
                        VALUES %s
                        ON CONFLICT ({})
                        DO UPDATE SET {}
                    """).format(
                        sql.Identifier(table_name),
                        insert_columns,
                        sql.Identifier(primary_key),
                        update_clause
                    ).as_string(conn)

                    batch = []

                    for row in reader:
                        batch.append(
                            tuple(row[column] for column in columns)
                        )

                        if len(batch) >= 1000:
                            execute_values(
                                cursor,
                                insert_query,
                                batch,
                                page_size=1000
                            )

                            total_rows += len(batch)
                            batch = []

                    if batch:
                        execute_values(
                            cursor,
                            insert_query,
                            batch,
                            page_size=1000
                        )

                        total_rows += len(batch)

            conn.commit()

            duration = time.perf_counter() - start_clock

            record_batch_log(
                source_file,
                start_time,
                duration,
                total_rows,
                "Success"
            )

            print(
                f"Successfully loaded {total_rows} rows "
                f"into raw.{table_name}"
            )

        except Exception as error:

            conn.rollback()

            duration = time.perf_counter() - start_clock

            try:
                record_batch_log(
                    source_file,
                    start_time,
                    duration,
                    total_rows,
                    "Failed",
                    str(error)
                )
            except Exception as log_error:
                print(f"Could not save failure log: {log_error}")

            raise

        finally:
            conn.close()

    master = load_csv.override(
        task_id="load_master_data"
    )(
        FILES[0]["file"],
        FILES[0]["table"],
        FILES[0]["pk"]
    )

    warehouse = load_csv.override(
        task_id="load_warehouse_data"
    )(
        FILES[1]["file"],
        FILES[1]["table"],
        FILES[1]["pk"]
    )

    sales = load_csv.override(
        task_id="load_pharmacy_sales_data"
    )(
        FILES[2]["file"],
        FILES[2]["table"],
        FILES[2]["pk"]
    )

    demand = load_csv.override(
        task_id="load_hospital_demand_data"
    )(
        FILES[3]["file"],
        FILES[3]["table"],
        FILES[3]["pk"]
    )

    master >> warehouse >> sales >> demand

