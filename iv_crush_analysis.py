import os
import pandas as pd
import numpy as np
from scipy.stats import norm
from scipy.optimize import brentq
from datetime import datetime, timedelta

# === CONFIG ===
EARNINGS_DIR = "earnings_data"
FLATFILES_DIR = "polygon_flat_files/us_options_opra"
OUTPUT_DIR = "options_data"
RISK_FREE_RATE = 0.05

os.makedirs(OUTPUT_DIR, exist_ok=True)

# === Black-Scholes helpers ===
def black_scholes_call(S, K, T, r, sigma):
    if T <= 0 or sigma <= 0:
        return 0
    d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    return S * norm.cdf(d1) - K * np.exp(-r * T) * norm.cdf(d2)

def black_scholes_put(S, K, T, r, sigma):
    if T <= 0 or sigma <= 0:
        return 0
    d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    return K * np.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)

def implied_vol(price, S, K, T, r, opt_type='call'):
    intrinsic = max(S - K, 0) if opt_type == 'call' else max(K - S, 0)
    if price <= 0 or price < intrinsic:
        return None
    try:
        fn = black_scholes_call if opt_type == 'call' else black_scholes_put
        return brentq(lambda x: fn(S, K, T, r, x) - price, 0.001, 5.0)
    except:
        return None

# === Parse option ticker ===
def parse_option_ticker(ticker):
    try:
        t = ticker.split(":")[1]
        # Example: O:AAPL241220C00190000 → symbol=AAPL, expiry=2024-12-20, type=call, strike=190.0
        for i in range(len(t)):
            if t[i:i+6].isdigit():
                symbol = t[:i]
                expiry_raw = t[i:i+6]
                expiry = datetime.strptime("20" + expiry_raw, "%Y%m%d").strftime("%Y-%m-%d")
                opt_type = "call" if t[i+6] == "C" else "put"
                strike = int(t[i+7:]) / 1000.0
                return symbol, expiry, opt_type, strike
    except:
        return None, None, None, None
    return None, None, None, None

# === Load flatfile ===
def load_flatfile(path):
    try:
        return pd.read_csv(path, compression="infer")
    except Exception as e:
        print(f"⚠️ Error loading {path}: {e}")
        return None

# === Collect available flatfile dates ===
flatfile_dates = []
for f in os.listdir(FLATFILES_DIR):
    name = os.path.splitext(f)[0]
    try:
        d = datetime.strptime(name, "%Y-%m-%d")
        flatfile_dates.append(d)
    except:
        continue

flatfile_dates = sorted(flatfile_dates)
print(f"📂 Found {len(flatfile_dates)} flatfiles in {FLATFILES_DIR}")
if flatfile_dates:
    print(f"   → Earliest: {flatfile_dates[0].date()}, Latest: {flatfile_dates[-1].date()}")

# === Main Loop ===
for file in os.listdir(EARNINGS_DIR):
    if not file.endswith(".csv"):
        continue

    symbol = file.split("_")[0].upper()
    earnings_path = os.path.join(EARNINGS_DIR, file)
    df_earnings = pd.read_csv(earnings_path)

    print(f"\n{'='*70}\nAnalyzing {symbol}\n{'='*70}")

    records = []

    for _, row in df_earnings.iterrows():
        try:
            # Convert aware → naive to allow comparison
            earn_date = pd.to_datetime(str(row["EarningsDate"]), utc=False).tz_localize(None)
        except Exception as e:
            print(f"⚠️ Could not parse {row.get('EarningsDate')}: {e}")
            continue

        # Convert all flatfile dates to naive
        flatfile_naive = [d.replace(tzinfo=None) for d in flatfile_dates]

        # Find nearest flatfiles before and after earnings
        pre_date = max([d for d in flatfile_naive if d <= earn_date], default=None)
        post_date = min([d for d in flatfile_naive if d >= earn_date], default=None)

        if not pre_date or not post_date:
            print(f"⚠️ Skipping {earn_date.date()} — no nearby flatfiles found")
            continue

        pre_file = os.path.join(FLATFILES_DIR, pre_date.strftime("%Y-%m-%d") + ".csv")
        post_file = os.path.join(FLATFILES_DIR, post_date.strftime("%Y-%m-%d") + ".csv")

        df_pre = load_flatfile(pre_file)
        df_post = load_flatfile(post_file)
        if df_pre is None or df_post is None:
            continue

        # Filter only this symbol's options
        df_pre_symbol = df_pre[df_pre["ticker"].astype(str).str.startswith(f"O:{symbol}")]
        df_post_symbol = df_post[df_post["ticker"].astype(str).str.startswith(f"O:{symbol}")]

        if df_pre_symbol.empty or df_post_symbol.empty:
            print(f"⚠️ No {symbol} options found around {earn_date.date()}")
            continue

        for _, opt in df_pre_symbol.iterrows():
            ticker = opt["ticker"]
            price_pre = opt["close"]
            post_match = df_post_symbol[df_post_symbol["ticker"] == ticker]

            if post_match.empty:
                continue

            price_post = float(post_match.iloc[0]["close"])
            sym, expiry, opt_type, strike = parse_option_ticker(ticker)
            if not expiry:
                continue

            # Approximate stock as ATM
            S = strike
            expiry_date = datetime.strptime(expiry, "%Y-%m-%d")
            T_pre = (expiry_date - pre_date).days / 365
            T_post = (expiry_date - post_date).days / 365

            iv_pre = implied_vol(price_pre, S, strike, T_pre, RISK_FREE_RATE, opt_type)
            iv_post = implied_vol(price_post, S, strike, T_post, RISK_FREE_RATE, opt_type)
            iv_crush = (iv_pre - iv_post) * 100 if iv_pre and iv_post else None

            records.append({
                "symbol": symbol,
                "earn_date": earn_date.date(),
                "ticker": ticker,
                "as_of_pre": pre_date.date(),
                "as_of_post": post_date.date(),
                "price_pre": price_pre,
                "price_post": price_post,
                "price_change": price_post - price_pre,
                "implied_vol_pre": iv_pre,
                "implied_vol_post": iv_post,
                "iv_crush": iv_crush,
                "expiry": expiry,
                "strike": strike,
                "type": opt_type,
                "pre_file": os.path.basename(pre_file),
                "post_file": os.path.basename(post_file)
            })

    # Save per-symbol output
    if records:
        out_path = os.path.join(OUTPUT_DIR, f"{symbol}_iv_crush.csv")
        pd.DataFrame(records).to_csv(out_path, index=False)
        print(f"✅ Saved {out_path} ({len(records)} rows)")
    else:
        print(f"⚠️ No valid records for {symbol}")

print("\n🎉 IV Crush analysis complete.")
