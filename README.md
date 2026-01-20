# WA Energy - WEM Data Collection

Collects electricity market data from Western Australia's Wholesale Electricity Market (WEM) using the [OpenElectricity API](https://openelectricity.org.au).

## Features

- Fetches hourly energy generation data by fuel type (solar, wind, gas, etc.)
- Fetches hourly wholesale electricity prices
- Fetches hourly demand data
- Outputs data as CSV files for analysis

## Installation

Requires Python 3.11+ and [uv](https://github.com/astral-sh/uv).

```bash
# Clone and install
git clone <repo-url>
cd wa-energy
uv sync
```

## Configuration

1. Get an API key from [OpenElectricity](https://platform.openelectricity.org.au)
2. Create a `.env` file:

```bash
cp .env.example .env
# Edit .env and add your API key
```

## Usage

```bash
# Run the data collection
uv run collect-wem
```

This will fetch 2024 data and save to the `data/` directory:
- `wem_generation_2024.csv` - Generation by fuel type (~85k records)
- `wem_price_2024.csv` - Wholesale prices (~8.8k records)
- `wem_demand_2024.csv` - Demand data (~8.8k records)
- `wem_combined_2024.csv` - All data combined
- `raw/` - Raw JSON API responses for debugging

## Project Structure

```
wa-energy/
├── src/wa_energy/
│   ├── __init__.py
│   └── collect_wem_data.py    # Main collection script
├── data/                       # Output data (gitignored)
├── api_context.txt             # API documentation notes
├── pyproject.toml              # Project config
├── .env                        # API key (gitignored)
└── .env.example                # Template for .env
```

## API Notes

Key learnings from integrating with the OpenElectricity API v4:

### Endpoints
- **Network data** (energy/power): `GET /v4/data/network/{network_code}`
- **Market data** (price/demand): `GET /v4/market/network/{network_code}`

### Parameter Format
The API uses **snake_case** parameters (not camelCase):
```
date_start=2024-01-01T00:00:00
date_end=2024-01-31T23:59:59
secondary_grouping=fueltech
```

### Date Format
Dates must be **timezone-naive** in network local time (Perth/AWST for WEM):
```
# Correct
date_start=2024-01-01T00:00:00

# Wrong - will cause "Date start must be timezone naive" error
date_start=2024-01-01T00:00:00+08:00
```

### Valid Intervals
`5m`, `1h`, `1d`, `7d`, `1M`, `3M`, `season`, `1y`, `fy`

Note: `30m` is NOT supported despite appearing in some documentation.

### Authentication
```
Authorization: Bearer {API_KEY}
```

See [api_context.txt](api_context.txt) for full API documentation.

## Development

```bash
# Run with uv
uv run python -m wa_energy.collect_wem_data

# Or use the installed script
uv run collect-wem
```
