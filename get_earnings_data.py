import yfinance as yf
import pandas as pd
import sys

TICKER = sys.argv[1] if len(sys.argv) > 1 else "AAPL"
LIMIT = 100
OUTFILE = f"{TICKER}_earnings_yf.csv"

tkr = yf.Ticker(TICKER)
df = tkr.get_earnings_dates(limit=LIMIT)

if df is None or df.empty:
    raise SystemExit(f"No earnings data returned for {TICKER}")

# Move index to column and robustly name it "EarningsDate"
df = df.reset_index()
df.rename(columns={df.columns[0]: "EarningsDate"}, inplace=True)

# Normalize types
df["EarningsDate"] = pd.to_datetime(df["EarningsDate"]).dt.date

# Optional: standardize other column names
df.columns = [c.replace(" ", "_").replace("(", "").replace(")", "") for c in df.columns]

df = df.sort_values("EarningsDate").reset_index(drop=True)
df.to_csv(OUTFILE, index=False)

print(f"Saved {len(df)} earnings events to {OUTFILE}")
print(df.tail(10))
