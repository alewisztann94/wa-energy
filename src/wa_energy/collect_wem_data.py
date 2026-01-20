"""
WEM Data Collection Script
Collects 1 year of data (Jan 2024 - Dec 2024) from OpenElectricity API
- 1-hour intervals (API supports: 5m, 1h, 1d, 7d, 1M, 3M, season, 1y, fy)
- 36 API calls total (12 months × 3 metric types)
"""

import json
import os
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# API Configuration
API_BASE = "https://api.openelectricity.org.au/v4"
NETWORK_DATA_URL = f"{API_BASE}/data/network/WEM"  # For energy/power data
MARKET_DATA_URL = f"{API_BASE}/market/network/WEM"  # For price/demand data
API_KEY = os.getenv("OPENELECTRICITY_API_KEY")
# Valid intervals: 5m, 1h, 1d, 7d, 1M, 3M, season, 1y, fy
INTERVAL = "1h"
OUTPUT_DIR = Path("data")

# Date ranges for each month in 2024
# API requires timezone-naive datetimes in network local time (Perth/AWST)
MONTH_RANGES = [
    ("2024-01-01T00:00:00", "2024-01-31T23:59:59"),
    ("2024-02-01T00:00:00", "2024-02-29T23:59:59"),  # 2024 is a leap year
    ("2024-03-01T00:00:00", "2024-03-31T23:59:59"),
    ("2024-04-01T00:00:00", "2024-04-30T23:59:59"),
    ("2024-05-01T00:00:00", "2024-05-31T23:59:59"),
    ("2024-06-01T00:00:00", "2024-06-30T23:59:59"),
    ("2024-07-01T00:00:00", "2024-07-31T23:59:59"),
    ("2024-08-01T00:00:00", "2024-08-31T23:59:59"),
    ("2024-09-01T00:00:00", "2024-09-30T23:59:59"),
    ("2024-10-01T00:00:00", "2024-10-31T23:59:59"),
    ("2024-11-01T00:00:00", "2024-11-30T23:59:59"),
    ("2024-12-01T00:00:00", "2024-12-31T23:59:59"),
]


def fetch_network_data(
    metrics: list,
    date_start: str,
    date_end: str,
    secondary_grouping: str | None = None,
) -> dict:
    """
    Fetch network data (energy/power) from OpenElectricity API

    Args:
        metrics: List of metrics to fetch (e.g., ["energy"], ["power"])
        date_start: Timezone-naive datetime in network time (e.g., "2024-01-01T00:00:00")
        date_end: Timezone-naive datetime in network time (e.g., "2024-01-31T23:59:59")
        secondary_grouping: Optional grouping (e.g., "fueltech" for generation by fuel type)

    Returns:
        API response as dictionary
    """
    params = {
        "metrics": ",".join(metrics),
        "interval": INTERVAL,
        "date_start": date_start,
        "date_end": date_end,
    }

    if secondary_grouping:
        params["secondary_grouping"] = secondary_grouping

    headers = {}
    if API_KEY:
        headers["Authorization"] = f"Bearer {API_KEY}"

    print(f"  Fetching {metrics} from {date_start} to {date_end}...")

    response = requests.get(NETWORK_DATA_URL, params=params, headers=headers, timeout=30)
    response.raise_for_status()

    return response.json()


def fetch_market_data(
    metrics: list,
    date_start: str,
    date_end: str,
) -> dict:
    """
    Fetch market data (price/demand) from OpenElectricity API

    Args:
        metrics: List of metrics to fetch (e.g., ["price"], ["demand"])
        date_start: Timezone-naive datetime in network time (e.g., "2024-01-01T00:00:00")
        date_end: Timezone-naive datetime in network time (e.g., "2024-01-31T23:59:59")

    Returns:
        API response as dictionary
    """
    params = {
        "metrics": ",".join(metrics),
        "interval": INTERVAL,
        "date_start": date_start,
        "date_end": date_end,
    }

    headers = {}
    if API_KEY:
        headers["Authorization"] = f"Bearer {API_KEY}"

    print(f"  Fetching {metrics} from {date_start} to {date_end}...")

    response = requests.get(MARKET_DATA_URL, params=params, headers=headers, timeout=30)
    response.raise_for_status()

    return response.json()


def parse_response_to_dataframe(response: dict, metric_type: str) -> pd.DataFrame:
    """
    Parse API response into a pandas DataFrame

    Actual API response structure:
    {
        "data": [{
            "network_code": "WEM",
            "metric": "energy",
            "unit": "MWh",
            "interval": "1h",
            "results": [{
                "name": "energy_total",
                "columns": {},
                "data": [
                    ["2024-01-01T00:00:00+08:00", 123.45],
                    ["2024-01-01T01:00:00+08:00", 234.56],
                    ...
                ]
            }]
        }]
    }

    Args:
        response: API response dictionary
        metric_type: Type of metric ("energy", "price", or "demand")

    Returns:
        DataFrame with parsed data
    """
    records = []

    if "data" not in response:
        return pd.DataFrame()

    for data_item in response["data"]:
        network = data_item.get("network_code", "WEM")
        metric = data_item.get("metric", metric_type)
        unit = data_item.get("unit", "")

        # Process results array
        for result in data_item.get("results", []):
            result_name = result.get("name", "")

            # Data is array of [timestamp, value] pairs
            for entry in result.get("data", []):
                if len(entry) >= 2:
                    timestamp, value = entry[0], entry[1]
                    records.append({
                        "timestamp": timestamp,
                        "value": value,
                        "metric": metric,
                        "unit": unit,
                        "network": network,
                        "series": result_name,
                    })

    return pd.DataFrame(records)


def collect_generation_data() -> pd.DataFrame:
    """Collect generation data by fuel type for all months"""
    print("\n=== Collecting Generation Data (by fuel type) ===")
    all_data = []
    raw_dir = OUTPUT_DIR / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    for i, (start, end) in enumerate(MONTH_RANGES, 1):
        month_label = start[:7]  # Extract "2024-01" from ISO datetime
        print(f"Month {i}/12: {month_label}")
        try:
            response = fetch_network_data(
                metrics=["energy"],
                date_start=start,
                date_end=end,
                secondary_grouping="fueltech",
            )
            df = parse_response_to_dataframe(response, "energy")
            all_data.append(df)

            # Save raw response for debugging
            raw_path = raw_dir / f"generation_{month_label}.json"
            raw_path.write_text(json.dumps(response, indent=2))

            time.sleep(1)  # Rate limiting

        except requests.exceptions.RequestException as e:
            print(f"  ERROR: {e}")
            continue

    if all_data:
        return pd.concat(all_data, ignore_index=True)
    return pd.DataFrame()


def collect_price_data() -> pd.DataFrame:
    """Collect price data for all months"""
    print("\n=== Collecting Price Data ===")
    all_data = []
    raw_dir = OUTPUT_DIR / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    for i, (start, end) in enumerate(MONTH_RANGES, 1):
        month_label = start[:7]  # Extract "2024-01" from ISO datetime
        print(f"Month {i}/12: {month_label}")
        try:
            response = fetch_market_data(
                metrics=["price"],
                date_start=start,
                date_end=end,
            )
            df = parse_response_to_dataframe(response, "price")
            all_data.append(df)

            # Save raw response
            raw_path = raw_dir / f"price_{month_label}.json"
            raw_path.write_text(json.dumps(response, indent=2))

            time.sleep(1)

        except requests.exceptions.RequestException as e:
            print(f"  ERROR: {e}")
            continue

    if all_data:
        return pd.concat(all_data, ignore_index=True)
    return pd.DataFrame()


def collect_demand_data() -> pd.DataFrame:
    """Collect demand data for all months"""
    print("\n=== Collecting Demand Data ===")
    all_data = []
    raw_dir = OUTPUT_DIR / "raw"
    raw_dir.mkdir(parents=True, exist_ok=True)

    for i, (start, end) in enumerate(MONTH_RANGES, 1):
        month_label = start[:7]  # Extract "2024-01" from ISO datetime
        print(f"Month {i}/12: {month_label}")
        try:
            response = fetch_market_data(
                metrics=["demand"],
                date_start=start,
                date_end=end,
            )
            df = parse_response_to_dataframe(response, "demand")
            all_data.append(df)

            # Save raw response
            raw_path = raw_dir / f"demand_{month_label}.json"
            raw_path.write_text(json.dumps(response, indent=2))

            time.sleep(1)

        except requests.exceptions.RequestException as e:
            print(f"  ERROR: {e}")
            continue

    if all_data:
        return pd.concat(all_data, ignore_index=True)
    return pd.DataFrame()


def main():
    """Main execution function"""
    print("=" * 60)
    print("WEM Data Collection Script")
    print("Period: January 2024 - December 2024")
    print(f"Interval: {INTERVAL}")
    print("Expected API calls: 36 (12 months × 3 metric types)")
    print("=" * 60)

    # Check for API key
    if not API_KEY:
        print("\nWARNING: OPENELECTRICITY_API_KEY not found in environment.")
        print("Create a .env file with: OPENELECTRICITY_API_KEY=your_key_here")
        print("Proceeding without authentication (may have rate limits)...\n")

    # Create output directory
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUTPUT_DIR / "raw").mkdir(parents=True, exist_ok=True)

    start_time = datetime.now()

    # Collect all data
    generation_df = collect_generation_data()
    price_df = collect_price_data()
    demand_df = collect_demand_data()

    # Save processed data
    print("\n=== Saving Processed Data ===")

    if not generation_df.empty:
        gen_path = OUTPUT_DIR / "wem_generation_2024.csv"
        generation_df.to_csv(gen_path, index=False)
        print(f"Generation data: {len(generation_df)} records -> {gen_path}")

    if not price_df.empty:
        price_path = OUTPUT_DIR / "wem_price_2024.csv"
        price_df.to_csv(price_path, index=False)
        print(f"Price data: {len(price_df)} records -> {price_path}")

    if not demand_df.empty:
        demand_path = OUTPUT_DIR / "wem_demand_2024.csv"
        demand_df.to_csv(demand_path, index=False)
        print(f"Demand data: {len(demand_df)} records -> {demand_path}")

    # Summary
    elapsed = datetime.now() - start_time
    print("\n" + "=" * 60)
    print("COLLECTION COMPLETE")
    print(f"Time elapsed: {elapsed}")
    print(f"Generation records: {len(generation_df)}")
    print(f"Price records: {len(price_df)}")
    print(f"Demand records: {len(demand_df)}")
    print("=" * 60)

    # Also save a combined dataset
    if not generation_df.empty or not price_df.empty or not demand_df.empty:
        combined_path = OUTPUT_DIR / "wem_combined_2024.csv"
        combined_df = pd.concat([generation_df, price_df, demand_df], ignore_index=True)
        combined_df.to_csv(combined_path, index=False)
        print(f"\nCombined dataset saved: {combined_path}")


if __name__ == "__main__":
    main()
