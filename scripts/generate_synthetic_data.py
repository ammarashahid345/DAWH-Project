from pathlib import Path
import pandas as pd
import numpy as np

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"
STAGING_DIR = BASE_DIR / "data" / "synthetic_staging"

rng = np.random.default_rng(42)

SOURCE_FILES = [
    "01_master_data.csv",
    "02_manufacturing_data.csv",
    "03_manufacturer_delivery_data.csv",
    "04_warehouse_data.csv",
    "05_pharmacy_sales_data.csv",
    "06_hospital_demand_data.csv",
    "07_iot_streaming_data.csv",
]


def load_source_data():
    data = {}
    for name in SOURCE_FILES:
        path = RAW_DIR / name
        if not path.exists():
            raise FileNotFoundError(f"Missing file: {name}")
        data[name] = pd.read_csv(path, dtype=str)
        print(f"{name}: {len(data[name]):,} original rows")
    return data


def generate_master_records(master, refresh_number):
    template_ids = ["MED001", "MED007", "MED055"]

    first_id = 64 if refresh_number == 1 else 67
    effective_date = "2026-01-01" if refresh_number == 1 else "2026-02-01"

    numeric_gtins = [
        int(x) for x in master["GTIN14"].dropna()
        if str(x).isdigit() and len(str(x)) == 14
    ]
    next_gtin = max(numeric_gtins) + 1

    records = []

    for offset, template_id in enumerate(template_ids):
        number = first_id + offset
        row = master.loc[
            master["MedicineID"] == template_id
        ].iloc[0].copy()

        row["MedicineID"] = f"MED{number:03d}"
        row["InternalSKU"] = f"SKU-08-{number:04d}"
        row["GTIN14"] = f"{next_gtin + (refresh_number - 1) * 3 + offset:014d}"
        row["GenericName"] = f"Synthetic Medicine {number:03d}"
        row["FictionalBrandName"] = f"SyntheticBrand{number:03d}"
        row["EffectiveFromDate"] = effective_date
        row["SourceSystem"] = "SYNTHETIC_GENERATOR"

        records.append(row)

    return pd.DataFrame(records, columns=master.columns)


def generate_manufacturing_records(
    manufacturing, synthetic_master, refresh_number
):
    template_ids = ["MED001", "MED007", "MED055"]
    records = []

    start_number = 562 + (refresh_number - 1) * 3
    base_date = pd.Timestamp(
        "2026-01-15" if refresh_number == 1 else "2026-02-15"
    )

    for offset, template_id in enumerate(template_ids):
        medicine = synthetic_master.iloc[offset]

        original = manufacturing.loc[
            manufacturing["MedicineID"] == template_id
        ].iloc[0].copy()

        number = start_number + offset
        plant = medicine["PrimaryPlantID"]

        planned = int(original["PlannedQuantityPacks"])
        actual = max(100, int(planned * rng.uniform(0.98, 1.02)))
        rejected = int(actual * rng.uniform(0.005, 0.02))
        released = actual - rejected

        start_date = base_date + pd.Timedelta(days=offset * 2)
        completion_date = start_date + pd.Timedelta(days=8 + offset)
        release_date = completion_date + pd.Timedelta(days=1)

        unit_cost = float(medicine["UnitManufacturingCostUSD"])
        total_cost = round(actual * unit_cost * rng.uniform(0.95, 1.05), 2)

        original_material = float(original["MaterialCostUSD"])
        original_labor = float(original["LaborCostUSD"])
        original_overhead = float(original["OverheadCostUSD"])
        original_total = original_material + original_labor + original_overhead

        if original_total > 0:
            material = round(total_cost * original_material / original_total, 2)
            labor = round(total_cost * original_labor / original_total, 2)
            overhead = round(total_cost - material - labor, 2)
        else:
            material = round(total_cost * 0.65, 2)
            labor = round(total_cost * 0.20, 2)
            overhead = round(total_cost - material - labor, 2)

        shelf_life = int(medicine["ShelfLifeMonths"])
        expiry_date = release_date + pd.DateOffset(months=shelf_life)

        row = original.astype(object).copy()

        row["ManufacturingOrderID"] = f"MO-2026-{number:05d}"
        row["ProductionBatchID"] = f"B26-{plant}-{number:06d}"
        row["MedicineID"] = medicine["MedicineID"]
        row["GTIN14"] = medicine["GTIN14"]
        row["PlantID"] = plant

        row["OrderDate"] = (start_date - pd.Timedelta(days=5)).strftime("%Y-%m-%d")
        row["PlannedStartDate"] = start_date.strftime("%Y-%m-%d")
        row["ActualStartDate"] = start_date.strftime("%Y-%m-%d")
        row["PlannedCompletionDate"] = completion_date.strftime("%Y-%m-%d")
        row["ActualCompletionDate"] = completion_date.strftime("%Y-%m-%d")

        row["PlannedQuantityPacks"] = planned
        row["ActualQuantityPacks"] = actual
        row["RejectedQuantityPacks"] = rejected
        row["ReleasedQuantityPacks"] = released
        row["YieldPct"] = round(released / actual * 100, 2)

        row["BatchStatus"] = "Released"
        row["QualityTestStatus"] = "Passed"
        row["QualitySampleDate"] = completion_date.strftime("%Y-%m-%d")
        row["BatchReleaseDate"] = release_date.strftime("%Y-%m-%d")
        row["ExpiryDate"] = expiry_date.strftime("%Y-%m-%d")

        row["StandardBatchSizePacks"] = medicine["StandardBatchSizePacks"]
        row["ManufacturingLeadTimeDays"] = medicine["ManufacturingLeadTimeDays"]

        row["TotalManufacturingCostUSD"] = round(total_cost, 2)
        row["MaterialCostUSD"] = material
        row["LaborCostUSD"] = labor
        row["OverheadCostUSD"] = overhead
        row["CostPerReleasedPackUSD"] = round(total_cost / released, 4)

        row["DelayDays"] = 0
        row["DelayReason"] = pd.NA
        row["DeviationFlag"] = "N"
        row["DeviationCategory"] = pd.NA
        row["SourceSystem"] = "SYNTHETIC_GENERATOR"
        row["RecordCreatedTimestampUTC"] = (
            release_date + pd.Timedelta(hours=2)
        ).strftime("%Y-%m-%dT%H:%M:%SZ")

        records.append(row)

    return pd.DataFrame(records, columns=manufacturing.columns)


def main():
    data = load_source_data()

    master = data["01_master_data.csv"]
    manufacturing = data["02_manufacturing_data.csv"]

    for refresh_number in (1, 2):
        cycle = f"refresh_{refresh_number}"
        output_dir = STAGING_DIR / cycle
        output_dir.mkdir(parents=True, exist_ok=True)

        synthetic_master = generate_master_records(
            master, refresh_number
        )

        synthetic_manufacturing = generate_manufacturing_records(
            manufacturing, synthetic_master, refresh_number
        )

        synthetic_master.to_csv(
            output_dir / "01_master_data.csv", index=False
        )

        synthetic_manufacturing.to_csv(
            output_dir / "02_manufacturing_data.csv", index=False
        )

        print(
            f"\n{cycle}: "
            f"{len(synthetic_master)} medicines, "
            f"{len(synthetic_manufacturing)} manufacturing batches generated."
        )

    print("\nMaster and Manufacturing generation completed.")
    print("Original source CSV files were not modified.")


if __name__ == "__main__":
    main()
