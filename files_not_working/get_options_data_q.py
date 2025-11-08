# from massive import RESTClient
# from datetime import datetime, timedelta
# from dateutil import parser
# import pandas as pd
# import os, time, signal, json

# API_KEY = "aQM_P6K_4kZjebUDUXQsC3nednoTpYRH"
# client = RESTClient(API_KEY)

# EARNINGS_DIR = "earnings_data"
# OUTPUT_DIR = "options_data"
# CACHE_PATH = "trade_cache.json"
# THROTTLE = 0.4

# os.makedirs(OUTPUT_DIR, exist_ok=True)

# # === Simple cache for trade prices ===
# try:
#     with open(CACHE_PATH, "r") as f:
#         trade_cache = json.load(f)
# except FileNotFoundError:
#     trade_cache = {}

# def save_cache():
#     with open(CACHE_PATH, "w") as f:
#         json.dump(trade_cache, f)

# # === Timeout setup ===
# class TimeoutException(Exception): pass
# def handler(signum, frame): raise TimeoutException()
# signal.signal(signal.SIGALRM, handler)

# def safe_call(fn, *args, **kwargs):
#     """Run a Massive API call safely with 10s timeout."""
#     signal.alarm(10)
#     try:
#         return fn(*args, **kwargs)
#     except TimeoutException:
#         print("   ⚠️ Timed out call, skipping...")
#         return None
#     except Exception as e:
#         print(f"   ⚠️ API call failed: {e}")
#         return None
#     finally:
#         signal.alarm(0)

# # === Fetch trade price (cached + safe) ===
# def fetch_trade_price(ticker: str, date_str: str):
#     """Fetch the last trade price for an option contract on a given date."""
#     print(f"    • Fetching trade price for {ticker} on {date_str}")
#     try:
#         trades = list(client.list_trades(
#             ticker,
#             params={"date": date_str, "limit": 1, "order": "desc"},
#         ))
#         if trades and hasattr(trades[0], "price"):
#             return trades[0].price
#     except Exception as e:
#         print(f"⚠️ Trade fetch failed for {ticker} on {date_str}: {e}")
#     time.sleep(THROTTLE)
#     return None


# # === Core processing ===
# DTE_TARGET, DTE_WINDOW = 45, 10

# for file in os.listdir(EARNINGS_DIR):
#     if not file.endswith(".csv"):
#         continue

#     symbol = file.split("_")[0].upper()
#     earnings = pd.read_csv(os.path.join(EARNINGS_DIR, file))
#     results = []

#     print(f"\n==================== {symbol} ====================")

#     for _, row in earnings.iterrows():
#         earn_date_raw = str(row["EarningsDate"])
#         try:
#             dt = parser.parse(earn_date_raw).replace(tzinfo=None)
#         except Exception as e:
#             print(f"⚠️ Could not parse date: {earn_date_raw} ({e})")
#             continue

#         for label, target_dt in {"pre": dt - timedelta(days=1), "post": dt + timedelta(days=1)}.items():
#             target_str = target_dt.strftime("%Y-%m-%d")
#             start_exp = (target_dt + timedelta(days=DTE_TARGET - DTE_WINDOW)).strftime("%Y-%m-%d")
#             end_exp = (target_dt + timedelta(days=DTE_TARGET + DTE_WINDOW)).strftime("%Y-%m-%d")

#             print(f"  {label.upper()} snapshot {target_str}: expiries {start_exp} → {end_exp}")

#             # === Fetch up to 50 contracts (avoid pagination) ===
#             contracts = []
#             try:
#                 for i, c in enumerate(client.list_options_contracts(
#                     symbol,
#                     params={
#                         "as_of": target_str,
#                         "expiration_date.gte": start_exp,
#                         "expiration_date.lte": end_exp,
#                         "limit": 50,
#                         "order": "asc",
#                         "sort": "strike_price",
#                     },
#                 )):
#                     contracts.append(c)
#                     if i >= 50:
#                         break
#             except Exception as e:
#                 print(f"   ⚠️ Error fetching contracts: {e}")
#                 continue

#             if not contracts:
#                 print(f"   ⚠️ No contracts for {symbol} on {target_str}")
#                 continue

#             # === Pick 3 near-ATM contracts ===
#             rows = []
#             for c in contracts[:3]:
#                 try:
#                     trade_price = fetch_trade_price(c.ticker, target_str)
#                     rows.append({
#                         "ticker": c.ticker,
#                         "strike": c.strike_price,
#                         "expiry": c.expiration_date,
#                         "price": trade_price,
#                         "context": label,
#                         "symbol": symbol,
#                         "earn_date": earn_date_raw,
#                         "as_of": target_str,
#                     })
#                 except Exception as e:
#                     print(f"    ⚠️ Error parsing trade: {e}")

#             results.extend(rows)
#             print(f"   ✅ Added {len(rows)} contracts for {label} snapshot.")

#     # === Build dataframe and compute crush ===
#     df = pd.DataFrame(results)
#     if df.empty:
#         print(f"⚠️ No valid data collected for {symbol}")
#         continue

#     df_pre = df[df["context"] == "pre"].set_index("ticker")
#     df_post = df[df["context"] == "post"].set_index("ticker")
#     merged = df_pre.join(df_post, lsuffix="_pre", rsuffix="_post", how="inner")

#     merged["price_change"] = merged["price_pre"] - merged["price_post"]

#     out_path = os.path.join(OUTPUT_DIR, f"{symbol}_45DTE_trades_safe.csv")
#     merged.reset_index().to_csv(out_path, index=False)
#     print(f"\n✅ Saved {len(merged)} matched contracts for {symbol} → {out_path}")











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
FETCH_TIMEOUT = 6   # seconds per trade fetch

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

            print(f"{label.upper()} snapshot {target_str}: expiries {start_exp} → {end_exp}")

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

            # Select 3 contracts closest to the money (if price info available)
            contracts = sorted(
                contracts,
                key=lambda c: getattr(c, "strike_price", 0)
            )[:3]

            print(f"  • Fetching trade price for {len(contracts)} selected contracts...")

            contracts_data = []
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {
                    executor.submit(fetch_option_trade_price, c.ticker, target_str): c.ticker
                    for c in contracts
                }
                for i, future in enumerate(as_completed(futures), start=1):
                    ticker = futures[future]
                    try:
                        price = future.result(timeout=FETCH_TIMEOUT)
                    except Exception as e:
                        print(f"⚠️ Timeout or error fetching {ticker}: {e}")
                        price = None

                    print(f"    ({i}/{len(futures)}) → {ticker}: {price}")
                    contracts_data.append({
                        "ticker": ticker,
                        "trade_price": price,
                        "strike": getattr(next((c for c in contracts if c.ticker == ticker), None), "strike_price", None),
                        "expiry": getattr(next((c for c in contracts if c.ticker == ticker), None), "expiration_date", None),
                        "context": label,
                        "symbol": symbol,
                        "earn_date": earn_date_raw,
                        "as_of": target_str
                    })

            print(f"✅ Added {len(contracts_data)} contracts for {label} snapshot.")
            results.extend(contracts_data)

        print(f"  ✅ Collected {len(results)} total records so far for {symbol}")

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
