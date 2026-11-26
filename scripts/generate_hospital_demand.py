import pandas as pd
import numpy as np
from pathlib import Path

BASE = Path(__file__).resolve().parents[1]
RAW = BASE / "data/raw"
STAGING = BASE / "data/synthetic_staging"

SOURCE = RAW / "06_hospital_demand_data.csv"
MASTER = RAW / "01_master_data.csv"

TEMPLATE_MEDICINE = {
    "MED064": "MED001",
    "MED065": "MED007",
    "MED066": "MED055",
    "MED067": "MED001",
    "MED068": "MED007",
    "MED069": "MED055",
}

NUMERIC_COLUMNS = [
    "ForecastDemandPacks", "RequestedQuantityPacks",
    "ApprovedQuantityPacks", "ReceivedQuantityPacks",
    "EmergencyRequestQuantityPacks", "OpeningStockPacks",
    "DispensedQuantityPacks", "ConsumedQuantityPacks",
    "WastageQuantityPacks", "ClosingStockPacks",
    "UnfulfilledDemandPacks", "StockoutDays",
    "ServiceLevelPct", "AverageLeadTimeDays",
    "DemandTrendIndex"
]

def integer(value):
    return max(0, int(round(float(value))))

def generate_refresh(refresh_number):
    rng = np.random.default_rng(2026 + refresh_number)

    source = pd.read_csv(SOURCE)
    master = pd.read_csv(MASTER, dtype=str)

    # Use December 2025 as the starting template for every hospital.
    latest = source[source["DemandMonth"] == "01-12-2025"].copy()

    # Load the synthetic medicine records generated in earlier steps.
    medicine_frames = [master]
    for refresh in range(1, refresh_number + 1):
        master_file = (
            STAGING / f"refresh_{refresh}" / "01_master_data.csv"
        )
        if master_file.exists():
            medicine_frames.append(pd.read_csv(master_file, dtype=str))

    catalogue = pd.concat(medicine_frames, ignore_index=True)
    catalogue = catalogue.drop_duplicates("MedicineID", keep="last")
    catalogue = catalogue.set_index("MedicineID", drop=False)

    hospitals = sorted(latest["HospitalID"].unique())

    if refresh_number == 1:
        medicine_ids = list(master["MedicineID"].dropna().unique())
        medicine_ids += ["MED064", "MED065", "MED066"]
        demand_month = "01-01-2026"
    else:
        medicine_ids = list(master["MedicineID"].dropna().unique())
        medicine_ids += [
            "MED064", "MED065", "MED066",
            "MED067", "MED068", "MED069"
        ]
        demand_month = "01-02-2026"

    records = []

    for hospital in hospitals:
        for medicine in medicine_ids:
            template_medicine = TEMPLATE_MEDICINE.get(
                medicine, medicine
            )

            match = latest[
                (latest["HospitalID"] == hospital) &
                (latest["MedicineID"] == template_medicine)
            ]

            if match.empty:
                raise ValueError(
                    f"No template found for {hospital}, {template_medicine}"
                )

            row = match.iloc[0].copy()

            # Use the corresponding master record for medicine attributes.
            if medicine not in catalogue.index:
                raise ValueError(f"Medicine {medicine} missing from master data")

            medicine_row = catalogue.loc[medicine]

            row["MedicineID"] = medicine
            for col in ["GTIN14", "GenericName", "DiseaseCategory"]:
                if col in medicine_row.index:
                    row[col] = medicine_row[col]

            row["DemandMonth"] = demand_month
            row["HospitalDemandRecordID"] = (
                f"SYN-HDEM-2026-{refresh_number}-"
                f"{len(records) + 1:05d}"
            )
            row["SourceSystem"] = "SYNTHETIC_GENERATOR"

            # Carry forward the previous month's closing stock.
            opening = integer(match.iloc[0]["ClosingStockPacks"])
            row["OpeningStockPacks"] = opening

            # Apply small variations to historical demand quantities.
            forecast = integer(
                float(match.iloc[0]["ForecastDemandPacks"]) *
                rng.uniform(0.90, 1.10)
            )
            requested = max(
                forecast,
                integer(
                    float(match.iloc[0]["RequestedQuantityPacks"]) *
                    rng.uniform(0.90, 1.10)
                )
            )

            approved_ratio = (
                float(match.iloc[0]["ApprovedQuantityPacks"]) /
                max(1, float(match.iloc[0]["RequestedQuantityPacks"]))
            )
            approved = min(
                requested,
                integer(requested * np.clip(
                    approved_ratio * rng.uniform(0.97, 1.03), 0, 1
                ))
            )

            received_ratio = (
                float(match.iloc[0]["ReceivedQuantityPacks"]) /
                max(1, float(match.iloc[0]["ApprovedQuantityPacks"]))
            )
            received = min(
                approved,
                integer(approved * np.clip(
                    received_ratio * rng.uniform(0.97, 1.03), 0, 1
                ))
            )

            emergency = integer(
                float(match.iloc[0]["EmergencyRequestQuantityPacks"]) *
                rng.uniform(0.80, 1.20)
            )

            dispensed = integer(
                float(match.iloc[0]["DispensedQuantityPacks"]) *
                rng.uniform(0.90, 1.10)
            )

            wastage = integer(
                float(match.iloc[0]["WastageQuantityPacks"]) *
                rng.uniform(0.80, 1.20)
            )

            # Ensure the stock balance never becomes negative.
            available = opening + received - wastage
            dispensed = min(dispensed, max(0, available))

            consumed_ratio = (
                float(match.iloc[0]["ConsumedQuantityPacks"]) /
                max(1, float(match.iloc[0]["DispensedQuantityPacks"]))
            )
            consumed = min(
                dispensed,
                integer(dispensed * np.clip(
                    consumed_ratio * rng.uniform(0.98, 1.02), 0, 1
                ))
            )

            closing = opening + received - dispensed - wastage

            row["ForecastDemandPacks"] = forecast
            row["RequestedQuantityPacks"] = requested
            row["ApprovedQuantityPacks"] = approved
            row["ReceivedQuantityPacks"] = received
            row["EmergencyRequestQuantityPacks"] = emergency
            row["DispensedQuantityPacks"] = dispensed
            row["ConsumedQuantityPacks"] = consumed
            row["WastageQuantityPacks"] = wastage
            row["ClosingStockPacks"] = closing

            # Retain source-style unfulfilled demand and service-level values.
            row["UnfulfilledDemandPacks"] = integer(
                float(match.iloc[0]["UnfulfilledDemandPacks"]) *
                rng.uniform(0.80, 1.20)
            )
            row["StockoutDays"] = integer(
                float(match.iloc[0]["StockoutDays"])
            )
            row["ServiceLevelPct"] = round(
                float(match.iloc[0]["ServiceLevelPct"]), 2
            )
            row["AverageLeadTimeDays"] = round(
                float(match.iloc[0]["AverageLeadTimeDays"]) *
                rng.uniform(0.95, 1.05), 2
            )
            row["DemandTrendIndex"] = round(
                float(match.iloc[0]["DemandTrendIndex"]) *
                rng.uniform(0.97, 1.03), 3
            )

            # Preserve MedicationRequestStatus and FormularyStatus from the historical template.

            records.append(row)

    result = pd.DataFrame(records)

    # Preserve the source column order.
    result = result[source.columns]

    output_dir = STAGING / f"refresh_{refresh_number}"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "06_hospital_demand_data.csv"
    result.to_csv(output_file, index=False)

    print(f"\nRefresh {refresh_number} generated successfully")
    print(f"Demand month: {demand_month}")
    print(f"Rows generated: {len(result)}")
    print(f"Hospitals: {result.HospitalID.nunique()}")
    print(f"Medicines: {result.MedicineID.nunique()}")
    print(f"Unique record IDs: {result.HospitalDemandRecordID.nunique()}")
    print(f"Output: {output_file}")

if __name__ == "__main__":
    generate_refresh(1)
    generate_refresh(2)
