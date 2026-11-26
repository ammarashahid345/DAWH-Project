from pathlib import Path
import pandas as pd
import numpy as np

BASE = Path(__file__).resolve().parent.parent
RAW = BASE / "data" / "raw"
STAGING = BASE / "data" / "synthetic_staging"
rng = np.random.default_rng(2028)


def main():
    source = pd.read_csv(RAW / "05_pharmacy_sales_data.csv", dtype=str)

    for cycle_no in (1, 2):
        folder = STAGING / f"refresh_{cycle_no}"

        batches = pd.read_csv(folder / "02_manufacturing_data.csv", dtype=str)
        warehouse = pd.read_csv(folder / "04_warehouse_data.csv", dtype=str)
        medicines = pd.read_csv(folder / "01_master_data.csv", dtype=str)

        records = []

        for i, (_, batch) in enumerate(batches.iterrows()):
            medicine = medicines.loc[
                medicines.MedicineID == batch.MedicineID
            ].iloc[0]

            lot = warehouse.loc[
                warehouse.ProductionBatchID == batch.ProductionBatchID
            ].iloc[0]

            template = source.loc[
                source.MedicineID == (
                    "MED001" if i == 0 else
                    "MED007" if i == 1 else "MED055"
                )
            ].iloc[0].astype(object).copy()

            quantity = int(rng.integers(5, 31))
            unit_price = float(medicine.RecommendedRetailPriceUSD)
            gross = round(quantity * unit_price, 2)

            discount_rate = float(rng.choice([0, 0.02, 0.05, 0.10]))
            discount = round(gross * discount_rate, 2)
            net = round(gross - discount, 2)

            cost_per_pack = float(batch.CostPerReleasedPackUSD)
            cost = round(quantity * cost_per_pack, 2)
            margin = round(net - cost, 2)

            sales_date = pd.Timestamp(
                "2026-02-10" if cycle_no == 1 else "2026-03-10"
            ) + pd.Timedelta(days=i * 3)

            transaction_no = (cycle_no - 1) * 3 + i + 1

            template["SalesTransactionID"] = (
                f"SYN-SALE-2026-{transaction_no:05d}"
            )
            template["MedicineID"] = batch.MedicineID
            template["GTIN14"] = medicine.GTIN14
            template["ProductionBatchID"] = batch.ProductionBatchID
            template["WarehouseLotID"] = lot.WarehouseLotID

            template["SalesDate"] = str(
                (sales_date - pd.Timestamp("1899-12-30")).days
            )

            # Preserve the template time but align timestamp with the new sales date
            time_fraction = float(template["SalesTimestampUTC"]) % 1
            sales_timestamp = sales_date + pd.to_timedelta(time_fraction, unit="D")
            template["SalesTimestampUTC"] = str(
                (sales_timestamp - pd.Timestamp("1899-12-30")) / pd.Timedelta(days=1)
            )
            template["QuantitySoldPacks"] = str(quantity)
            template["UnitRetailPriceUSD"] = str(unit_price)
            template["GrossSalesAmountUSD"] = str(gross)
            template["DiscountAmountUSD"] = str(discount)
            template["NetSalesAmountUSD"] = str(net)
            template["CostOfGoodsSoldUSD"] = str(cost)
            template["GrossMarginUSD"] = str(margin)

            template["ReturnedQuantityPacks"] = "0"
            template["SourceSystem"] = "SYNTHETIC_GENERATOR"

            records.append(template)

        output = pd.DataFrame(records, columns=source.columns)
        output.to_csv(folder / "05_pharmacy_sales_data.csv", index=False)

        print(
            f"refresh_{cycle_no}: "
            f"{len(output)} synthetic pharmacy sales generated."
        )

    print("Pharmacy sales generation completed. Original CSV unchanged.")


if __name__ == "__main__":
    main()
