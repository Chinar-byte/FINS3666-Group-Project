from massive import RESTClient
from datetime import datetime, timedelta
from dateutil import parser
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import time
import os

API_KEY = "aQM_P6K_4kZjebUDUXQsC3nednoTpYRH"
client = RESTClient(API_KEY)

EARNINGS_DIR = "earnings_data"
OUTPUT_DIR = "options_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# === PARAMETERS ===
DTE_TARGET = 45
DTE_WINDOW = 10
THROTTLE = 0.4
MAX_WORKERS = 5
FETCH_TIMEOUT = 15  # seconds, manual timeout guard

# ---------------- HELPER ----------------
def fetch_option_trade_price(ticker: str, date_str: str):
    """Fetch the last trade price for an option contract on a given date."""
    try:
        trades = list(client.list_trades(
            ticker,
            params={"date": date_str, "limit": 1, "order": "desc"},
        ))
        if trades and hasattr(trades[0], "price"):
            return trades[0].price
    except Exception as e:
        print(f"⚠️ Trade fetch failed for {ticker} on {date_str}: {e}")
    time.sleep(THROTTLE)
    return None


# ---------------- MAIN LOOP ----------------
for file in os.listdir(EARNINGS_DIR):
    if not file.endswith(".csv"):
        continue

    symbol = file.split("_")[0].upper()
    earnings = pd.read_csv(os.path.join(EARNINGS_DIR, file))
    results = []

    print(f"\n==================== {symbol} ====================")

    for _, row in earnings.iterrows():
        earn_date_raw = str(row["EarningsDate"])
        try:
            dt = parser.parse(earn_date_raw).replace(tzinfo=None)
        except Exception as e:
            print(f"⚠️ Could not parse date: {earn_date_raw} ({e})")
            continue

        print(f"\n🔍 Earnings date: {earn_date_raw}")

        for label, target_dt in {"pre": dt - timedelta(days=1),
                                 "post": dt + timedelta(days=1)}.items():
            target_str = target_dt.strftime("%Y-%m-%d")
            start_exp = (target_dt + timedelta(days=DTE_TARGET - DTE_WINDOW)).strftime("%Y-%m-%d")
            end_exp = (target_dt + timedelta(days=DTE_TARGET + DTE_WINDOW)).strftime("%Y-%m-%d")

            print(f"  {label.upper()} snapshot: {target_str} → expirations {start_exp} – {end_exp}")

            try:
                # Fetch available contracts (45±10 DTE)
                contracts = list(client.list_options_contracts(
                    symbol,
                    params={
                        "as_of": target_str,
                        "expiration_date.gte": start_exp,
                        "expiration_date.lte": end_exp,
                        "limit": 10,
                        "order": "asc",
                        "sort": "strike_price",
                    },
                ))
            except Exception as e:
                print(f"   ⚠️ Error fetching contracts: {e}")
                continue

            if not contracts:
                print(f"  ⚠️ No contracts found for {symbol} on {target_str}")
                continue

            # Extract metadata
            contracts_data = [{
                "ticker": c.ticker,
                "strike": c.strike_price,
                "expiry": c.expiration_date,
                "contract_type": c.contract_type,
                "exchange": getattr(c, "primary_exchange", None),
            } for c in contracts if hasattr(c, "ticker")]

            df = pd.DataFrame(contracts_data)
            print(f"  → Retrieved {len(df)} contracts for {symbol} on {target_str}")

            # Concurrent trade fetches with manual timeout protection
            prices = []
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {executor.submit(fetch_option_trade_price, t, target_str): t for t in df["ticker"]}
                for future in as_completed(futures, timeout=FETCH_TIMEOUT * len(df)):
                    prices.append(future.result())
                    print(f"    • Fetched trade price for {future.result()} appended to {prices}")

            df["trade_price"] = prices
            df["context"] = label
            df["symbol"] = symbol
            df["earn_date"] = earn_date_raw
            df["as_of"] = target_str

            results.extend(df.to_dict("records"))

        print(f"  ✅ Collected {len(results)} total records for {symbol}")

    # === Combine PRE and POST ===
    df_all = pd.DataFrame(results)
    if df_all.empty:
        print(f"⚠️ No valid data collected for {symbol}.")
        continue

    df_pre = df_all[df_all["context"] == "pre"].set_index("ticker")
    df_post = df_all[df_all["context"] == "post"].set_index("ticker")

    merged = df_pre.join(df_post, lsuffix="_pre", rsuffix="_post", how="inner")
    merged["premium_change"] = merged["trade_price_pre"] - merged["trade_price_post"]

    out_path = os.path.join(OUTPUT_DIR, f"{symbol}_45DTE_trades.csv")
    merged.reset_index().to_csv(out_path, index=False)
    print(f"\n✅ Saved {len(merged)} matched pre/post contracts for {symbol} → {out_path}")
