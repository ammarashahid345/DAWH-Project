import argparse
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw" / "07_iot_streaming_data.csv"
STAGING = ROOT / "data" / "synthetic_staging"

ACTIVE_DEVICES = [
    "IOT-WH001-AMB-01",
    "IOT-WH001-AMB-02",
    "IOT-WH001-COLD-01",
    "IOT-WH001-COLD-02",
    "IOT-WH002-AMB-01",
    "IOT-WH002-AMB-02",
    "IOT-WH002-COLD-01",
    "IOT-WH002-COLD-02",
    "IOT-WH003-AMB-01",
    "IOT-WH003-AMB-02",
]

PERIODS = {
    1: ("2026-01-01", "2026-02-01"),
    2: ("2026-02-01", "2026-03-01"),
}


def main(refresh):
    if refresh not in PERIODS:
        raise ValueError("Refresh must be 1 or 2.")

    rng = np.random.default_rng(202611 + refresh)
    original = pd.read_csv(RAW, dtype={"SensorReadingID": str})

    required = {
        "SensorReadingID", "DeviceID", "EventTimestampUTC",
        "EventSequenceNumber", "TemperatureC", "RelativeHumidityPct",
        "StorageMinTempC", "StorageMaxTempC", "MaxHumidityPct",
    }
    missing = required - set(original.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    original["EventTimestampUTC"] = pd.to_datetime(
        original["EventTimestampUTC"], utc=True
    )
    original["IngestionTimestampUTC"] = pd.to_datetime(
        original["IngestionTimestampUTC"], utc=True
    )

    start, end = PERIODS[refresh]
    timestamps = pd.date_range(
        start=start, end=end, freq="3h", inclusive="left", tz="UTC"
    )

    max_id = int(
        original["SensorReadingID"].str.extract(r"(\d+)$")[0].dropna().astype(int).max()
    ) + (refresh - 1) * 2480

    rows = []
    for device in ACTIVE_DEVICES:
        history = original[original["DeviceID"] == device].sort_values(
            "EventTimestampUTC"
        )
        if history.empty:
            raise ValueError(f"No historical records found for {device}")

        last = history.iloc[-1]
        if last["EventTimestampUTC"] != pd.Timestamp("2025-12-31 21:00:00+00:00"):
            raise ValueError(f"Unexpected last timestamp for {device}")

        previous_sequence = int(history["EventSequenceNumber"].max()) + (refresh - 1) * 248
        previous_battery = float(last["BatteryLevelPct"])

        # Use historical device-specific distributions.
        temp_history = history["TemperatureC"].astype(float)
        humidity_history = history["RelativeHumidityPct"].astype(float)
        signal_history = history["SignalStrengthDbm"].astype(float)
        shock_history = history["ShockG"].astype(float)
        latency_history = history["DataLatencySeconds"].astype(float)

        is_cold = "-COLD-" in device
        temp_mean = 4.8 if is_cold else 21.0
        temp_sd = 1.15 if is_cold else 2.0
        humidity_mean = 60.0 if is_cold else 52.0
        humidity_sd = 4.0 if is_cold else 3.5

        for timestamp in timestamps:
            previous_sequence += 1
            max_id += 1

            sampled_index = int(rng.integers(0, len(history)))
            sampled_history = history.iloc[sampled_index]
            temperature = round(float(sampled_history['TemperatureC']), 2)
            humidity = round(float(sampled_history['RelativeHumidityPct']), 2)

            min_temp = float(last["StorageMinTempC"])
            max_temp = float(last["StorageMaxTempC"])
            max_humidity = float(last["MaxHumidityPct"])

            temp_breach = temperature < min_temp or temperature > max_temp
            humidity_breach = humidity > max_humidity
            threshold_breach = temp_breach or humidity_breach

            severity = str(sampled_history['AlertSeverity'])

            door_open = "Y" if rng.random() < 0.008 else "N"
            shock = round(
                float(np.clip(rng.normal(0.06, 0.025), 0, 0.12)), 3
            )

            battery = max(
                93.9,
                min(100.0, previous_battery - rng.uniform(0.0002, 0.0012))
            )
            previous_battery = battery

            signal = int(np.clip(
                round(rng.normal(float(signal_history.mean()), 8)), -88, -48
            ))

            latency = int(np.clip(
                round(rng.lognormal(np.log(45), 0.8)), 2, 2400
            ))
            late = "Y" if latency > 300 else "N"
            ingestion_time = timestamp + pd.Timedelta(seconds=latency)

            quality = "Suspect" if (
                rng.random() < 0.005 or shock > 0.11
            ) else "Valid"

            row = last.to_dict()
            row.update({
                "SensorReadingID": f"IOTR-{max_id:010d}",
                "DeviceID": device,
                "EventTimestampUTC": timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "IngestionTimestampUTC": ingestion_time.strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                ),
                "EventSequenceNumber": previous_sequence,
                "TemperatureC": temperature,
                "RelativeHumidityPct": humidity,
                "DoorOpenFlag": door_open,
                "ShockG": shock,
                "BatteryLevelPct": round(battery, 2),
                "SignalStrengthDbm": signal,
                "TemperatureBreachFlag": "Y" if temp_breach else "N",
                "HumidityBreachFlag": "Y" if humidity_breach else "N",
                "ThresholdBreachFlag": "Y" if threshold_breach else "N",
                "AlertSeverity": severity,
                "ReadingQuality": quality,
                "DataLatencySeconds": latency,
                "LateArrivalFlag": late,
                "SourceStream": "mqtt/pharma/warehouse/environment",
                "ShipmentID": np.nan,
                "SSCC18": np.nan,
            })
            rows.append(row)

    generated = pd.DataFrame(rows, columns=original.columns)

    # Basic validation before writing.
    assert len(generated) == len(ACTIVE_DEVICES) * len(timestamps)
    assert generated["SensorReadingID"].is_unique
    assert not generated.duplicated(["DeviceID", "EventTimestampUTC"]).any()
    assert generated["TemperatureBreachFlag"].eq(
        np.where(
            (generated["TemperatureC"] < generated["StorageMinTempC"]) |
            (generated["TemperatureC"] > generated["StorageMaxTempC"]),
            "Y", "N"
        )
    ).all()
    assert generated["HumidityBreachFlag"].eq(
        np.where(
            generated["RelativeHumidityPct"] > generated["MaxHumidityPct"],
            "Y", "N"
        )
    ).all()

    output_dir = STAGING / f"refresh_{refresh}"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "07_iot_streaming_data.csv"
    generated.to_csv(output_file, index=False)

    print(f"Refresh {refresh} generated successfully.")
    print(f"Period: {start} to {end} (end exclusive)")
    print(f"Active devices: {len(ACTIVE_DEVICES)}")
    print(f"Rows generated: {len(generated)}")
    print(f"Unique sensor IDs: {generated['SensorReadingID'].nunique()}")
    print(f"Temperature breaches: {generated['TemperatureBreachFlag'].eq('Y').sum()}")
    print(f"Humidity breaches: {generated['HumidityBreachFlag'].eq('Y').sum()}")
    print(f"Output: {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--refresh", type=int, choices=[1, 2], required=True)
    args = parser.parse_args()
    main(args.refresh)
