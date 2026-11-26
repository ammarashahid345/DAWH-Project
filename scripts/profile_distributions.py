import os
import pandas as pd

BASE_DIR = os.path.expanduser("~/airflow-project/data/raw")
OUTPUT_DIR = os.path.expanduser("~/airflow-project/data/profiling")

FILES = [
    "01_master_data.csv",
    "02_manufacturing_data.csv",
    "03_manufacturer_delivery_data.csv",
    "04_warehouse_data.csv",
    "05_pharmacy_sales_data.csv",
    "06_hospital_demand_data.csv",
    "07_iot_streaming_data.csv"
]

os.makedirs(OUTPUT_DIR, exist_ok=True)

file_summary = []
null_summary = []
numeric_summary = []
category_summary = []
date_summary = []

for filename in FILES:
    path = os.path.join(BASE_DIR, filename)

    print(f"\nProfiling: {filename}")
    df = pd.read_csv(path, encoding="utf-8-sig", low_memory=False)

    file_summary.append({
        "file": filename,
        "rows": len(df),
        "columns": len(df.columns)
    })

    for col in df.columns:
        missing = int(df[col].isna().sum())
        null_summary.append({
            "file": filename,
            "column": col,
            "missing_count": missing,
            "missing_pct": round(missing / len(df) * 100, 3)
        })

        if pd.api.types.is_numeric_dtype(df[col]):
            s = df[col].dropna()
            if len(s):
                numeric_summary.append({
                    "file": filename,
                    "column": col,
                    "count": len(s),
                    "mean": s.mean(),
                    "median": s.median(),
                    "std": s.std(),
                    "min": s.min(),
                    "max": s.max()
                })

        elif pd.api.types.is_string_dtype(df[col]):
            values = df[col].dropna().astype(str)
            if len(values) and values.nunique() <= 100:
                counts = values.value_counts(normalize=True).head(10)
                for value, proportion in counts.items():
                    category_summary.append({
                        "file": filename,
                        "column": col,
                        "category": value,
                        "count": int((values == value).sum()),
                        "proportion_pct": round(proportion * 100, 3)
                    })

        if any(word in col.lower() for word in ["date", "timestamp", "month"]):
            dates = pd.to_datetime(df[col], errors="coerce").dropna()
            if len(dates):
                date_summary.append({
                    "file": filename,
                    "column": col,
                    "earliest": dates.min(),
                    "latest": dates.max(),
                    "valid_dates": len(dates)
                })

    print(f"Rows: {len(df):,} | Columns: {len(df.columns)}")

reports = {
    "file_summary.csv": file_summary,
    "missing_values.csv": null_summary,
    "numeric_distributions.csv": numeric_summary,
    "categorical_distributions.csv": category_summary,
    "date_ranges.csv": date_summary
}

for report, records in reports.items():
    pd.DataFrame(records).to_csv(
        os.path.join(OUTPUT_DIR, report), index=False
    )

print("\nBASELINE PROFILING COMPLETED")
print("Reports saved in:", OUTPUT_DIR)
print("\nFile summary:")
print(pd.DataFrame(file_summary).to_string(index=False))
