# import yfinance as yf
# import pandas as pd
# import sys

# TICKER = sys.argv[1] if len(sys.argv) > 1 else "AAPL"
# LIMIT = 100
# OUTFILE = f"{TICKER}_earnings_yf.csv"

# tkr = yf.Ticker(TICKER)
# df = tkr.get_earnings_dates(limit=LIMIT)

# if df is None or df.empty:
#     raise SystemExit(f"No earnings data returned for {TICKER}")

# # Move index to column and robustly name it "EarningsDate"
# df = df.reset_index()
# df.rename(columns={df.columns[0]: "EarningsDate"}, inplace=True)

# # Normalize types
# df["EarningsDate"] = pd.to_datetime(df["EarningsDate"]).dt.date

# # Optional: standardize other column names
# df.columns = [c.replace(" ", "_").replace("(", "").replace(")", "") for c in df.columns]

# df = df.sort_values("EarningsDate").reset_index(drop=True)
# df.to_csv(OUTFILE, index=False)

# print(f"Saved {len(df)} earnings events to {OUTFILE}")
# print(df.tail(10))


import sys
import yfinance as yf
import pandas as pd
import os

# Usage: python3 get_earnings_data.py AAPL
if len(sys.argv) < 2:
    print("❌ Usage: python3 get_earnings_data.py <TICKER>")
    sys.exit(1)

ticker = sys.argv[1].upper()
LIMIT = 20

print(f"📅 Fetching earnings dates for {ticker} (limit={LIMIT})")

tkr = yf.Ticker(ticker)
df = tkr.get_earnings_dates(limit=LIMIT)

# --- Defensive rename: handle both old and new versions of yfinance ---
if "Earnings Date" in df.columns:
    df.rename(columns={"Earnings Date": "EarningsDate"}, inplace=True)
elif "EarningsDate" not in df.columns:
    # Sometimes yfinance returns unnamed index with date values
    df.reset_index(inplace=True)
    if "Earnings Date" in df.columns:
        df.rename(columns={"Earnings Date": "EarningsDate"}, inplace=True)
    elif "index" in df.columns:
        df.rename(columns={"index": "EarningsDate"}, inplace=True)

# --- Validate result ---
if "EarningsDate" not in df.columns:
    print("❌ Could not find any valid earnings date column from yfinance output")
    print(df.head())
    sys.exit(1)

# --- Save ---
os.makedirs("earnings_data", exist_ok=True)
out_path = f"earnings_data/{ticker}_earnings.csv"
df.to_csv(out_path, index=False)
print(f"✅ Saved {out_path} ({len(df)} rows)")
