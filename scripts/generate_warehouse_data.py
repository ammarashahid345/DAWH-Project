from pathlib import Path
import pandas as pd
import numpy as np

BASE = Path(__file__).resolve().parent.parent
RAW = BASE / "data" / "raw"
STAGING = BASE / "data" / "synthetic_staging"
rng = np.random.default_rng(2027)


def main():
    source = pd.read_csv(
        RAW / "04_warehouse_data.csv", dtype=str
    )

    for cycle_no in (1, 2):
        folder = STAGING / f"refresh_{cycle_no}"

        batches = pd.read_csv(
            folder / "02_manufacturing_data.csv", dtype=str
        )
        deliveries = pd.read_csv(
            folder / "03_manufacturer_delivery_data.csv", dtype=str
        )

        records = []

        for i, (_, batch) in enumerate(batches.iterrows()):
            shipment = deliveries.iloc[i]

            template_medicine_id = (
                'MED001' if i == 0 else
                'MED007' if i == 1 else 'MED055'
            )
            template = source.loc[
                source.MedicineID == template_medicine_id
            ].iloc[0].astype(object).copy()

            snapshot_date = pd.Timestamp(
                "2026-02-05" if cycle_no == 1 else "2026-03-05"
            ) + pd.Timedelta(days=i * 3)

            quantity = int(shipment.ReceivedQuantityPacks)

            template["InventorySnapshotID"] = (
                f"SYN-INV-2026-{(cycle_no - 1) * 3 + i + 1:05d}"
            )
            template["WarehouseLotID"] = (
                f"SYN-LOT-2026-{(cycle_no - 1) * 3 + i + 1:04d}"
            )
            template["MedicineID"] = batch.MedicineID
            template["GTIN14"] = batch.GTIN14
            template["ProductionBatchID"] = batch.ProductionBatchID
            template["InboundShipmentID"] = shipment.ShipmentID

            template["SnapshotDate"] = str(
                (snapshot_date - pd.Timestamp("1899-12-30")).days
            )
            template["BatchExpiryDate"] = str(
                (pd.to_datetime(batch.ExpiryDate) -
                 pd.Timestamp("1899-12-30")).days
            )

            template["OpeningStockPacks"] = "0"
            template["ReceivedQuantityPacks"] = str(quantity)
            template["AvailableQuantityPacks"] = str(quantity)
            template["ReservedQuantityPacks"] = "0"
            template["DamagedQuantityPacks"] = "0"
            template["QuarantinedQuantityPacks"] = "0"

            template["SourceSystem"] = "SYNTHETIC_GENERATOR"

            records.append(template)

        output = pd.DataFrame(records, columns=source.columns)
        output.to_csv(
            folder / "04_warehouse_data.csv", index=False
        )

        print(
            f"refresh_{cycle_no}: "
            f"{len(output)} synthetic warehouse snapshots generated."
        )

    print("Warehouse generation completed. Original CSV unchanged.")


if __name__ == "__main__":
    main()
