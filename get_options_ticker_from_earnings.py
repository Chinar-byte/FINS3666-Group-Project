"""
Fetch Historical Option Contracts (as_of API) — Cached Version
--------------------------------------------------------------
For each symbol and earnings date:
  • Calls the Polygon/Massive REST API with `as_of`
    to get the full option chain as it existed that day.
  • Filters contracts within the DTE window (45±10 days).
  • Caches results to JSON so subsequent runs skip API calls.
  • Saves combined tickers for each symbol to ./option_tickers/.

Output:
  option_tickers/<SYMBOL>_tickers.csv
Cache:
  cache/<SYMBOL>/<YYYY-MM-DD>_<snapshot>.json
"""

import os
import json
import time
import pandas as pd
from massive import RESTClient
from datetime import datetime, timedelta
from dateutil import parser

# === CONFIG ===
API_KEY = "aQM_P6K_4kZjebUDUXQsC3nednoTpYRH"

EARNINGS_DIR = "earnings_data"
OUTPUT_DIR = "option_tickers"
CACHE_DIR = "cache"
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(CACHE_DIR, exist_ok=True)

DTE_TARGET = 45
DTE_WINDOW = 10
LIMIT = 2000
SLEEP = 0.2   # seconds between API calls

client = RESTClient(API_KEY)

def cache_path(symbol: str, date_str: str, label: str) -> str:
    sym_dir = os.path.join(CACHE_DIR, symbol)
    os.makedirs(sym_dir, exist_ok=True)
    return os.path.join(sym_dir, f"{date_str}_{label}.json")

def save_json(path: str, data):
    with open(path, "w") as f:
        json.dump(data, f)

def load_json(path: str):
    with open(path, "r") as f:
        return json.load(f)

def fetch_contracts(symbol, target_str, start_exp, end_exp, label):
    """Fetch contracts from cache if available, otherwise via API."""
    cache_file = cache_path(symbol, target_str, label)
    if os.path.exists(cache_file):
        data = load_json(cache_file)
        print(f"   💾 Loaded cached {len(data)} contracts for {symbol} {label} {target_str}")
        return data

    print(f"   🌐 Fetching contracts for {symbol} {label} {target_str} ...")
    try:
        contracts = list(client.list_options_contracts(
            symbol,
            params={
                "as_of": target_str,
                "expiration_date.gte": start_exp,
                "expiration_date.lte": end_exp,
                "limit": 200,
                "order": "asc",
                "sort": "strike_price",
            },
        ))
    except Exception as e:
        print(f"   ⚠️ API error: {e}")
        return []

    # Serialize minimal fields
    serialised = [{
        "ticker": c.ticker,
        "expiration_date": c.expiration_date,
        "strike_price": c.strike_price,
        "contract_type": c.contract_type
    } for c in contracts]

    save_json(cache_file, serialised)
    print(f"   ✅ Cached {len(serialised)} contracts → {cache_file}")
    time.sleep(SLEEP)
    return serialised

# === MAIN ===
print(f"\n{'='*70}\nHISTORICAL OPTION CONTRACT FETCHER (Cached)\n{'='*70}")

for file in os.listdir(EARNINGS_DIR):
    if not file.endswith(".csv"):
        continue

    symbol = file.split("_")[0].upper()
    earnings = pd.read_csv(os.path.join(EARNINGS_DIR, file))
    all_rows = []

    print(f"\n{'='*70}\n{symbol:^70}\n{'='*70}")

    for _, row in earnings.iterrows():
        earn_date_raw = str(row["EarningsDate"])
        try:
            dt = parser.parse(earn_date_raw).replace(tzinfo=None)
        except Exception as e:
            print(f"⚠️ Could not parse date {earn_date_raw}: {e}")
            continue

        for label, target_dt in {"pre": dt - timedelta(days=1),
                                 "post": dt + timedelta(days=1)}.items():
            target_str = target_dt.strftime("%Y-%m-%d")
            start_exp = (target_dt + timedelta(days=DTE_TARGET - DTE_WINDOW)).strftime("%Y-%m-%d")
            end_exp   = (target_dt + timedelta(days=DTE_TARGET + DTE_WINDOW)).strftime("%Y-%m-%d")

            print(f"\n🔹 {symbol} | {label.upper()} snapshot {target_str} | Expiries {start_exp} → {end_exp}")

            contracts = fetch_contracts(symbol, target_str, start_exp, end_exp, label)
            if not contracts:
                print(f"   ⚠️ No contracts for {symbol} {label} {target_str}")
                continue

            for c in contracts:
                all_rows.append({
                    "symbol": symbol,
                    "earnings_date": earn_date_raw,
                    "snapshot": label,
                    "snapshot_date": target_str,
                    "option_ticker": c["ticker"],
                    "expiration": c["expiration_date"],
                    "strike": c["strike_price"],
                    "type": c["contract_type"]
                })

    if all_rows:
        df = pd.DataFrame(all_rows)
        out_path = os.path.join(OUTPUT_DIR, f"{symbol}_tickers.csv")
        df.to_csv(out_path, index=False)
        print(f"\n✅ {symbol}: Saved {len(df)} contracts → {out_path}")
    else:
        print(f"⚠️ No data found for {symbol}")

print("\n🎉 Done! Cached contract data and tickers saved.")
