"""
WEM Demand Data Collection from AEMO ST-PASA Reports
Downloads and parses ST-PASA (Short Term Projected Assessment of System Adequacy) reports
from AEMO to extract WEM demand data for 2024.

Source: https://data.wa.aemo.com.au/public/market-data/st-pasa/
"""

import io
import os
import time
import zipfile
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

# Configuration
BASE_URL = "https://data.wa.aemo.com.au/public/market-data/st-pasa"
OUTPUT_DIR = Path("data")
RAW_DIR = OUTPUT_DIR / "raw" / "st-pasa"

# Date range for 2024
START_DATE = datetime(2024, 1, 1)
END_DATE = datetime(2024, 12, 31)


def get_zip_urls(date: datetime) -> list[str]:
    """
    Generate possible URLs for a ST-PASA zip file for a given date.
    Naming convention changed around June 2024 from spaces to underscores.
    Returns a list of URLs to try.
    """
    date_str = date.strftime("%Y-%m-%d")

    # Return both formats to try
    return [
        f"{BASE_URL}/{date_str}_ST-PASA_Report.zip",  # Newer underscore format
        f"{BASE_URL}/{date_str}%20ST-PASA%20Report.zip",  # URL-encoded space format
        f"{BASE_URL}/{date_str} ST-PASA Report.zip",  # Space format (may not work)
    ]


def download_and_extract_csv(date: datetime) -> pd.DataFrame | None:
    """
    Download a ST-PASA zip file and extract the CSV data.
    Returns a DataFrame with the demand data or None if failed.
    """
    urls = get_zip_urls(date)
    date_str = date.strftime("%Y-%m-%d")

    for url in urls:
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()

            # Extract CSV from zip
            with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
                # Find ST-PASA CSV by filename pattern
                csv_files = [f for f in zf.namelist() if f.endswith('.csv')]
                if not csv_files:
                    continue

                # Prefer files matching ST-PASA/Report pattern
                st_pasa_csvs = [f for f in csv_files if 'st-pasa' in f.lower() or 'report' in f.lower()]
                csv_name = st_pasa_csvs[0] if st_pasa_csvs else csv_files[0]
                with zf.open(csv_name) as csv_file:
                    # Read CSV with encoding handling
                    try:
                        df = pd.read_csv(csv_file, encoding='utf-8')
                    except UnicodeDecodeError:
                        csv_file.seek(0)
                        df = pd.read_csv(csv_file, encoding='latin-1')
                    return df

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                continue  # Try next URL format
            else:
                continue
        except Exception as e:
            continue

    print(f"  Not found: {date_str}")
    return None


def parse_demand_data(df: pd.DataFrame, source_date: datetime) -> list[dict]:
    """
    Parse the ST-PASA CSV to extract demand data.

    Old format columns (pre-June 2024):
    - Dispatch Interval: datetime (e.g., "10/01/2024 08:00")
    - Forecast Reference: demand in MW (central estimate)

    New format columns (June 2024+):
    - dispatchInterval: datetime (e.g., "01-07-2024 08:00")
    - ForecastOperationalDemandReference: demand in MW
    """
    records = []

    # Normalize column names to lowercase for case-agnostic matching
    col_mapping = {col.lower(): col for col in df.columns}

    # Detect column format
    if 'dispatch interval' in col_mapping:
        interval_col = col_mapping['dispatch interval']
        demand_col = col_mapping.get('forecast reference')
        date_format = '%d/%m/%Y %H:%M'
    elif 'dispatchinterval' in col_mapping:
        interval_col = col_mapping['dispatchinterval']
        demand_col = col_mapping.get('forecastoperationaldemandreference')
        date_format = '%d-%m-%Y %H:%M'
    else:
        return records

    if demand_col is None:
        return records

    for _, row in df.iterrows():
        try:
            # Parse the dispatch interval
            interval_str = str(row.get(interval_col, ''))
            if not interval_str or interval_str == 'nan':
                continue

            # Parse datetime
            try:
                timestamp = pd.to_datetime(interval_str, format=date_format)
            except:
                # Try alternative formats
                try:
                    timestamp = pd.to_datetime(interval_str, dayfirst=True)
                except:
                    continue

            # Get demand value
            demand_value = row.get(demand_col)
            if pd.isna(demand_value):
                continue

            records.append({
                'timestamp': timestamp.strftime('%Y-%m-%dT%H:%M:%S+08:00'),
                'value': float(demand_value),
                'metric': 'demand',
                'unit': 'MW',
                'network': 'WEM',
                'series': 'demand_reference',
                'source': 'AEMO_ST-PASA',
            })

        except Exception as e:
            continue

    return records


def collect_demand_data() -> pd.DataFrame:
    """
    Collect demand data for all of 2024 from AEMO ST-PASA reports.
    """
    print("=" * 60)
    print("WEM Demand Data Collection from AEMO ST-PASA Reports")
    print(f"Period: {START_DATE.strftime('%Y-%m-%d')} to {END_DATE.strftime('%Y-%m-%d')}")
    print("=" * 60)

    # Create output directories
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    all_records = []
    current_date = START_DATE
    days_processed = 0
    days_failed = 0

    while current_date <= END_DATE:
        date_str = current_date.strftime("%Y-%m-%d")

        if current_date.day == 1:
            print(f"\nProcessing {current_date.strftime('%B %Y')}...")

        df = download_and_extract_csv(current_date)

        if df is not None:
            records = parse_demand_data(df, current_date)
            all_records.extend(records)
            days_processed += 1

            # Save raw CSV for debugging (optional, just first of each month)
            if current_date.day == 1:
                raw_path = RAW_DIR / f"st-pasa_{date_str}.csv"
                df.to_csv(raw_path, index=False)
        else:
            days_failed += 1

        current_date += timedelta(days=1)

        # Rate limiting - be nice to AEMO servers
        time.sleep(0.5)

    print(f"\n\nDownloaded {days_processed} days, {days_failed} failed")

    if all_records:
        demand_df = pd.DataFrame(all_records)

        # Remove duplicates (same timestamp)
        demand_df = demand_df.drop_duplicates(subset=['timestamp'])

        # Sort by timestamp
        demand_df = demand_df.sort_values('timestamp')

        return demand_df

    return pd.DataFrame()


def aggregate_to_hourly(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate 30-minute demand data to hourly intervals.
    Averages the two 30-min values within each hour.
    """
    df = df.copy()
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # Floor to hour for grouping
    df['hour'] = df['timestamp'].dt.floor('h')

    # Group by hour and average the values
    hourly = df.groupby('hour').agg({
        'value': 'mean',
        'metric': 'first',
        'unit': 'first',
        'network': 'first',
        'series': 'first',
        'source': 'first',
    }).reset_index()

    # Format timestamp back to ISO format with timezone
    hourly['timestamp'] = hourly['hour'].dt.strftime('%Y-%m-%dT%H:%M:%S+08:00')
    hourly = hourly.drop(columns=['hour'])

    # Reorder columns
    hourly = hourly[['timestamp', 'value', 'metric', 'unit', 'network', 'series', 'source']]

    return hourly


def main():
    """Main execution function"""
    start_time = datetime.now()

    demand_df = collect_demand_data()

    if not demand_df.empty:
        # Save 30-min data
        demand_path_30m = OUTPUT_DIR / "wem_demand_aemo_2024_30m.csv"
        demand_df.to_csv(demand_path_30m, index=False)
        print(f"\n30-min demand data saved: {len(demand_df)} records -> {demand_path_30m}")

        # Aggregate to hourly and save
        hourly_df = aggregate_to_hourly(demand_df)
        demand_path = OUTPUT_DIR / "wem_demand_aemo_2024.csv"
        hourly_df.to_csv(demand_path, index=False)
        print(f"Hourly demand data saved: {len(hourly_df)} records -> {demand_path}")

        # Show sample of hourly data
        print("\nSample hourly data:")
        print(hourly_df.head(10).to_string())

        # Stats
        print(f"\nDate range: {hourly_df['timestamp'].min()} to {hourly_df['timestamp'].max()}")
        print(f"Demand range: {hourly_df['value'].min():.0f} - {hourly_df['value'].max():.0f} MW")
        print(f"Average demand: {hourly_df['value'].mean():.0f} MW")
    else:
        print("\nNo demand data collected!")

    elapsed = datetime.now() - start_time
    print(f"\nTime elapsed: {elapsed}")


if __name__ == "__main__":
    main()
