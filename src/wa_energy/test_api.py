"""
Quick API probe for OpenElectricity metrics without running the full collector.
"""

import os

import requests
from dotenv import load_dotenv

API_BASE = "https://api.openelectricity.org.au/v4"
NETWORK_DATA_URL = f"{API_BASE}/data/network/WEM"
MARKET_DATA_URL = f"{API_BASE}/market/network/WEM"

DEFAULT_START = "2024-01-01T00:00:00"
DEFAULT_END = "2024-01-01T23:59:59"
DEFAULT_INTERVAL = "1h"
DEMAND_INTERVALS = ["5m", "1h", "1d", "7d", "1M"]

# Metrics to probe (split by endpoint).
NETWORK_METRICS = [
    "power",
    "energy",
    "emissions",
    "renewable_proportion",
]

MARKET_METRICS = [
    "price",
    "market_value",
    "demand",
    "demand_energy",
]

# Load .env so OPENELECTRICITY_API_KEY is available when running via uv.
load_dotenv()


def fetch(url: str, metric: str, date_start: str, date_end: str, interval: str) -> dict:
    params = {
        "metrics": metric,
        "interval": interval,
        "date_start": date_start,
        "date_end": date_end,
    }
    headers = {}
    api_key = os.getenv("OPENELECTRICITY_API_KEY")
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    response = requests.get(url, params=params, headers=headers, timeout=30)
    response.raise_for_status()
    return response.json()


def count_non_null(response: dict) -> tuple[int, int]:
    total = 0
    non_null = 0
    for data_item in response.get("data", []):
        for result in data_item.get("results", []):
            for entry in result.get("data", []):
                if len(entry) >= 2:
                    total += 1
                    if entry[1] is not None:
                        non_null += 1
    return non_null, total


def probe_metrics(label: str, url: str, metrics: list, date_start: str, date_end: str, interval: str) -> None:
    print(f"\n=== {label} ===")
    for metric in metrics:
        try:
            response = fetch(url, metric, date_start, date_end, interval)
            non_null, total = count_non_null(response)
            print(f"{metric:22s} non_null={non_null} total={total}")
        except requests.RequestException as exc:
            print(f"{metric:22s} ERROR: {exc}")


def main() -> None:
    date_start = os.getenv("OE_DATE_START", DEFAULT_START)
    date_end = os.getenv("OE_DATE_END", DEFAULT_END)
    interval = os.getenv("OE_INTERVAL", DEFAULT_INTERVAL)

    print("OpenElectricity quick probe")
    print(f"Range: {date_start} -> {date_end}  Interval: {interval}")

    probe_metrics("Network metrics", NETWORK_DATA_URL, NETWORK_METRICS, date_start, date_end, interval)
    probe_metrics("Market metrics", MARKET_DATA_URL, MARKET_METRICS, date_start, date_end, interval)

    print("\n=== Demand intervals ===")
    for demand_interval in DEMAND_INTERVALS:
        label = f"interval={demand_interval}"
        print(f"\n{label}")
        probe_metrics(
            "demand metrics",
            MARKET_DATA_URL,
            ["demand", "demand_energy"],
            date_start,
            date_end,
            demand_interval,
        )


if __name__ == "__main__":
    main()
