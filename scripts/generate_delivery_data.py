from pathlib import Path
import pandas as pd
import numpy as np

BASE = Path(__file__).resolve().parent.parent
RAW = BASE / "data" / "raw"
STAGING = BASE / "data" / "synthetic_staging"
rng = np.random.default_rng(2026)


def main():
    source = pd.read_csv(
        RAW / "03_manufacturer_delivery_data.csv", dtype=str
    )

    for cycle_no in (1, 2):
        folder = STAGING / f"refresh_{cycle_no}"

        batches = pd.read_csv(
            folder / "02_manufacturing_data.csv", dtype=str
        )
        medicines = pd.read_csv(
            folder / "01_master_data.csv", dtype=str
        )

        records = []

        for i, (_, batch) in enumerate(batches.iterrows()):
            medicine = medicines.loc[
                medicines.MedicineID == batch.MedicineID
            ].iloc[0]

            template = source.loc[
                source.MedicineID == (
                    "MED001" if i == 0 else
                    "MED007" if i == 1 else "MED055"
                )
            ].iloc[0].astype(object).copy()

            shipped = int(batch.ReleasedQuantityPacks)
            damaged = int(shipped * rng.uniform(0, 0.003))
            short = int(shipped * rng.uniform(0, 0.002))
            received = shipped - damaged - short

            dispatch = pd.Timestamp(
                "2026-02-01" if cycle_no == 1 else "2026-03-01"
            ) + pd.Timedelta(days=i * 3)

            transit_hours = int(rng.choice([24, 48, 72, 96, 120]))
            actual_delivery = dispatch + pd.Timedelta(
                hours=transit_hours
            )
            promised_delivery = dispatch + pd.Timedelta(
                hours=transit_hours + 24
            )

            on_time = "Y" if actual_delivery <= promised_delivery else "N"
            in_full = "Y" if received == shipped else "N"
            otif = "Y" if on_time == "Y" and in_full == "Y" else "N"

            freight = round(
                float(template["FreightCostUSD"]) *
                (shipped / float(template["ShippedQuantityPacks"])),
                2
            )

            template["ShipmentID"] = (
                f"SYN-SHP-2026-{(cycle_no - 1) * 3 + i + 1:04d}"
            )
            template["ShipmentLineNumber"] = "1"
            template["ManufacturingOrderID"] = batch.ManufacturingOrderID
            template["ProductionBatchID"] = batch.ProductionBatchID
            template["MedicineID"] = batch.MedicineID
            template["GTIN14"] = medicine.GTIN14
            template["OriginPlantID"] = batch.PlantID

            template["DispatchDate"] = dispatch.strftime("%d-%m-%Y")
            template["PromisedDeliveryDate"] = promised_delivery.strftime("%d-%m-%Y")
            template["ActualDeliveryDate"] = actual_delivery.strftime("%d-%m-%Y")

            template["ShippedQuantityPacks"] = str(shipped)
            template["ReceivedQuantityPacks"] = str(received)
            template["DamagedQuantityPacks"] = str(damaged)
            template["ShortQuantityPacks"] = str(short)

            template["TransitHours"] = str(transit_hours)
            template["OnTimeDeliveryFlag"] = on_time
            template["InFullDeliveryFlag"] = in_full
            template["OTIFFlag"] = otif

            template["FreightCostUSD"] = str(freight)
            template["FreightCostPerPackUSD"] = str(
                round(freight / shipped, 4)
            )

            template["ShipmentStatus"] = "Delivered"
            template["TemperatureExcursionFlag"] = "N"
            template["ProofOfDeliveryFlag"] = "Y"
            template["DeliveryExceptionReason"] = pd.NA
            template["SourceSystem"] = "SYNTHETIC_GENERATOR"

            records.append(template)

        output = pd.DataFrame(records, columns=source.columns)
        output.to_csv(
            folder / "03_manufacturer_delivery_data.csv",
            index=False
        )

        print(
            f"refresh_{cycle_no}: "
            f"{len(output)} synthetic shipment lines generated."
        )

    print("Delivery generation completed. Original CSV unchanged.")


if __name__ == "__main__":
    main()
