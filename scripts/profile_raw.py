
import csv
import os

BASE_DIR = os.path.expanduser("~/airflow-project/data/raw")

FILES = {
    "01_master_data.csv": "MedicineID",
    "04_warehouse_data.csv": "InventorySnapshotID",
    "05_pharmacy_sales_data.csv": "SalesTransactionID",
    "06_hospital_demand_data.csv": "HospitalDemandRecordID"
}

for filename, primary_key in FILES.items():

    filepath = os.path.join(BASE_DIR, filename)

    print("\n" + "=" * 55)
    print("FILE:", filename)

    row_count = 0
    null_keys = 0
    duplicate_keys = 0
    seen_keys = set()

    with open(filepath, "r", encoding="utf-8-sig", newline="") as file:

        reader = csv.DictReader(file)

        if primary_key not in (reader.fieldnames or []):
            print("ERROR: Primary key column missing!")
            continue

        for row in reader:

            row_count += 1

            key = row[primary_key].strip()

            if not key:
                null_keys += 1

            elif key in seen_keys:
                duplicate_keys += 1

            else:
                seen_keys.add(key)

    print("Total Data Rows:", row_count)
    print("Primary Key:", primary_key)
    print("NULL Primary Keys:", null_keys)
    print("Duplicate Primary Keys:", duplicate_keys)

    print("Validation Complete")

