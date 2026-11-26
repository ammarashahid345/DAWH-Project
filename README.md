# Project-phase-1-DAWH
Sales and Demand Intelligence Ammara (36238), Zunaira(36225)
# Pharma Cold Chain – Data Warehouse & Business Intelligence Project

## Phase 1: Sales & Demand Intelligence (SDI)

### Project Overview
This project implements a data warehouse and business intelligence pipeline for pharmaceutical supply-chain Sales & Demand Intelligence. It uses Apache Airflow to orchestrate pipeline tasks, PostgreSQL to store raw, staging, and mart data, and Grafana to present monitoring and business intelligence dashboards.

The Phase 1 focus is on analysing hospital medicine demand and pharmacy sales performance.

### Technology Stack
- Apache Airflow 3.4.0 – workflow orchestration
- Docker Compose – local service orchestration
- PostgreSQL – raw, staging, and mart database layers
- Grafana – business intelligence and data-engineering monitoring dashboards
- Python – data generation and pipeline scripts
- SQL – transformations and analytical queries

### Data Warehouse Layers
The PostgreSQL database (`pharma_dwh`) contains these schemas:

- `raw` – source data tables
- `staging` – prepared staging tables
- `mart` – dimensional model for Sales & Demand Intelligence

The Phase 1 mart contains these dimension tables:
- `dim_date`
- `dim_hospital`
- `dim_location`
- `dim_medicine`
- `dim_pharmacy`
- `dim_warehouse`

And these fact tables:
- `fact_demand`
- `fact_sales`

### Data Sources
The project uses seven pharmaceutical supply-chain source datasets:
1. Master data
2. Manufacturing data
3. Manufacturer delivery data
4. Warehouse data
5. Pharmacy sales data
6. Hospital demand data
7. IoT streaming data

Phase 1 primarily analyses pharmacy sales and hospital demand.

### Analytical SQL Queries
The project includes three analytical SQL queries:

1. `query_01_hospital_monthly_demand.sql` – compares monthly hospital forecast demand with actual consumption and unfulfilled demand.
2. `query_02_forecast_accuracy_fulfilment.sql` – analyses forecast accuracy and request fulfilment by year, hospital type, and disease category.
3. `query_03_sales_channel_profitability.sql` – compares transactions, packs sold, net sales, and gross margin across sales channels.

### Grafana Dashboards
The Grafana dashboards developed for the project include:

- **Pharma Sales & Demand Intelligence** – hospital forecast vs. actual consumption, unfulfilled demand, sales-channel revenue and margin, and fast-/slow-moving medicines.
- **Pharma DWH – Batch Ingestion Monitoring** – batch load duration, rows processed per batch, and batch success rate.
- **Pharma DWH – Storage & Staging Monitoring** – table row counts, PostgreSQL storage footprint, date coverage, and source-domain row distribution.
- **Pharma Cold Chain – Sales & Demand Intelligence** – query performance monitoring, including average query execution time, daily active query count, and query error rate.

Dashboard screenshots or exported dashboard files can be added under `dashboards/` if included in this repository.

### Synthetic Data and Airflow
A synthetic-data refresh workflow is orchestrated through Apache Airflow. The workflow includes two refresh task groups (`refresh_1` and `refresh_2`) and was observed completing successfully in the Airflow UI.

> Note: A successful Airflow task status confirms task completion. Row-count changes in the raw and mart tables should be verified separately when demonstrating the data refresh.

### Repository Structure
Update this structure to match the folders and filenames actually uploaded:

```text
Pharma-DWH-Project/
├── airflow_dags/
├── scripts/
├── queries/
│   ├── query_01_hospital_monthly_demand.sql
│   ├── query_02_forecast_accuracy_fulfilment.sql
│   └── query_03_sales_channel_profitability.sql
├── reports/
├── dashboards/
├── DAWH.DWH.Template.xlsx
└── README.md
```

### Running the Project
The project was developed and run locally using Docker Compose and a Python virtual environment.

General startup sequence (adjust filenames/commands to match the files included in this repository):

1. Start the Docker Compose services from the project directory.
2. Ensure PostgreSQL and Airflow services are healthy.
3. Open the Airflow web interface and trigger the required DAG.
4. Connect to the `pharma_dwh` PostgreSQL database to inspect raw, staging, and mart tables.
5. Open Grafana to view the dashboards.

Do not commit local credentials, `.env` files, virtual environments, database volumes, or other secrets to the repository.

### Project Notes
- Database name: `pharma_dwh`
- PostgreSQL schemas: `raw`, `staging`, `mart`
- Grafana dashboards are hosted locally during development; local dashboard URLs are not publicly accessible unless separately deployed.
- This repository should contain the source code, SQL, reports, completed template, and approved demo link required for the Phase 1 submission.
