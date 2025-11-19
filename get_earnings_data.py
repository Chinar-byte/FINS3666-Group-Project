#!/usr/bin/env python3
import sys
import yfinance as yf
import pandas as pd
import os
from datetime import datetime

# ==========================================================
# 📅 Fetch earnings dates for a single ticker (2022–2025 only)
# ----------------------------------------------------------
# Usage: python3 get_earnings_data.py AAPL
# Saves: earnings_data/AAPL_earnings.csv
# ==========================================================

if len(sys.argv) < 2:
    print("❌ Usage: python3 get_earnings_data.py <TICKER>")
    sys.exit(1)

ticker = sys.argv[1].upper()
LIMIT = 50  # fetch a large window first
START_YEAR, END_YEAR = 2021, 2025

print(f"📅 Fetching earnings dates for {ticker} ({START_YEAR}-{END_YEAR})")

tkr = yf.Ticker(ticker)
df = tkr.get_earnings_dates(limit=LIMIT)

# --- Defensive rename to handle different yfinance versions ---
if df is None or df.empty:
    print(f"⚠️ No earnings data returned for {ticker}")
    sys.exit(0)

if "Earnings Date" in df.columns:
    df.rename(columns={"Earnings Date": "EarningsDate"}, inplace=True)
elif "EarningsDate" not in df.columns:
    df.reset_index(inplace=True)
    if "Earnings Date" in df.columns:
        df.rename(columns={"Earnings Date": "EarningsDate"}, inplace=True)
    elif "index" in df.columns:
        df.rename(columns={"index": "EarningsDate"}, inplace=True)

# --- Convert and filter by year range ---
df["EarningsDate"] = pd.to_datetime(df["EarningsDate"], errors="coerce")
df = df.dropna(subset=["EarningsDate"])

df = df[df["EarningsDate"].dt.year.between(START_YEAR, END_YEAR)]

if df.empty:
    print(f"⚠️ No earnings events found for {ticker} between {START_YEAR}-{END_YEAR}")
    sys.exit(0)

# --- Final cleanup ---
df = df.sort_values("EarningsDate").reset_index(drop=True)
os.makedirs("earnings_data", exist_ok=True)
out_path = f"earnings_data/{ticker}_earnings.csv"
df.to_csv(out_path, index=False)

print(f"✅ Saved {len(df)} earnings events to {out_path}")
print(df.tail(5))
