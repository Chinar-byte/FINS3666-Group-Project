#!/usr/bin/env python3
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from scipy.stats import norm
from scipy.optimize import brentq

# ================================================================
# CONFIG
# ================================================================
FLATFILES_DIR = "polygon_flat_files/us_options_opra"   
EARNINGS_DIR  = "earnings_data"
OUTPUT_DIR    = "options_data"
MASTER_OUT    = os.path.join(OUTPUT_DIR, "master_iv_rv_crush_with_price_change.csv")

RISK_FREE = 0.05
os.makedirs(OUTPUT_DIR, exist_ok=True)



# ================================================================
# IV FUNCTIONS
# ================================================================
def bs_call(S, K, T, r, sigma):
    if T <= 0 or sigma <= 0: 
        return 0
    d1 = (np.log(S/K) + (r + 0.5*sigma*sigma)*T) / (sigma*np.sqrt(T))
    d2 = d1 - sigma*np.sqrt(T)
    return S*norm.cdf(d1) - K*np.exp(-r*T)*norm.cdf(d2)

def bs_put(S, K, T, r, sigma):
    if T <= 0 or sigma <= 0: 
        return 0
    d1 = (np.log(S/K) + (r + 0.5*sigma*sigma)*T) / (sigma*np.sqrt(T))
    d2 = d1 - sigma*np.sqrt(T)
    return K*np.exp(-r*T)*norm.cdf(-d2) - S*norm.cdf(-d1)

def implied_vol(price, S, K, T, r, is_call):
    if price <= 0:
        return np.nan
    def f(sig):
        return (bs_call if is_call else bs_put)(S, K, T, r, sig) - price
    try:
        return brentq(f, 1e-6, 5.0)
    except:
        return np.nan



# ================================================================
# PARSE OPRA CODES SAFELY
# ================================================================
def parse_opra(opra):
    try:
        t = opra.split(":")[1]
        # Find YYYYMMDD
        for i in range(len(t)):
            if t[i:i+6].isdigit():
                symbol = t[:i]
                expiry = "20" + t[i:i+6]
                expiry = datetime.strptime(expiry, "%Y%m%d").strftime("%Y-%m-%d")
                cp = t[i+6]
                strike = int(t[i+7:]) / 1000
                return symbol, expiry, ("call" if cp == "C" else "put"), strike
    except:
        pass
    return None, None, None, None



# ================================================================
# LOAD FLATFILE
# ================================================================
def load_flatfile(date_str):
    path = os.path.join(FLATFILES_DIR, f"{date_str}.csv")
    if not os.path.exists(path):
        return None
    try:
        return pd.read_csv(path)
    except:
        return None



# ================================================================
# PURE PYTHON YANG–ZHANG RV (30-day)
# ================================================================
def yz_rv(open_, high, low, close):
    if len(close) < 2:
        return np.nan

    log_ho = np.log(high / open_)
    log_lo = np.log(low / open_)
    log_oc = np.log(close / open_)

    r_overnight = np.log(open_[1:] / close[:-1])
    r_close = np.log(close[1:] / close[:-1])

    k = 0.34 / (1.34 + (len(close) + 1)/(len(close) - 1))

    var_overnight = np.var(r_overnight, ddof=1)
    var_oc = np.var(log_oc, ddof=1)
    var_cc = np.var(r_close, ddof=1)
    var_rs = np.mean(log_ho * log_lo)

    yz = var_overnight + k*var_oc + (1-k)*var_cc - var_rs
    return np.sqrt(max(yz, 0))


def compute_rv30(ticker, earn_date):
    rows = []
    for offset in range(1, 31):
        day = (earn_date - timedelta(days=offset)).strftime("%Y-%m-%d")
        df = load_flatfile(day)
        if df is None:
            continue

        sub = df[df["ticker"].str.contains(f"O:{ticker}", na=False)]
        if sub.empty:
            continue

        rows.append({
            "open": sub["open"].median(),
            "high": sub["high"].median(),
            "low": sub["low"].median(),
            "close": sub["close"].median(),
        })

    if len(rows) < 10:
        return np.nan

    df_daily = pd.DataFrame(rows)
    return yz_rv(
        df_daily["open"].values,
        df_daily["high"].values,
        df_daily["low"].values,
        df_daily["close"].values
    )



# ================================================================
# FIXED ATM PICKER — NO GARBAGE STRIKES
# ================================================================
def pick_atm(df):

    # Remove garbage strikes (0.5, 1, 2, etc)
    m = df["strike"].median()

    # ⬅️ CRITICAL FIX — MAKE A REAL COPY
    filtered = df[(df["strike"] > 0.5*m) & (df["strike"] < 1.5*m)].copy()

    if filtered.empty:
        return None

    underlying = filtered["strike"].median()

    # This no longer triggers SettingWithCopyWarning
    filtered["strike_dist"] = (filtered["strike"] - underlying).abs()

    atm = filtered.sort_values("strike_dist").groupby("type").head(1)

    return atm if len(atm) == 2 else None




# ================================================================
# PROCESS ONE TICKER
# ================================================================
def process_ticker(ticker):
    earnings_csv = os.path.join(EARNINGS_DIR, f"{ticker}_earnings.csv")
    if not os.path.exists(earnings_csv):
        print(f"⚠️ Missing: {earnings_csv}")
        return pd.DataFrame()

    df_earn = pd.read_csv(earnings_csv)
    all_rows = []

    for _, row in df_earn.iterrows():
        earn_dt = pd.to_datetime(str(row["EarningsDate"])).date()
        dt = datetime.strptime(str(earn_dt), "%Y-%m-%d")

        pre = (dt - timedelta(days=1)).strftime("%Y-%m-%d")
        post = (dt + timedelta(days=1)).strftime("%Y-%m-%d")

        df_pre = load_flatfile(pre)
        df_post = load_flatfile(post)
        if df_pre is None or df_post is None:
            continue

        # Rough filter: just tickers containing O:TICKER
        def filter_exact(df):
            rough = df[df["ticker"].str.contains(f"O:{ticker}", na=False)].copy()
            if rough.empty:
                return rough

            parsed = rough["ticker"].apply(parse_opra)
            expanded = pd.DataFrame(parsed.tolist(), index=rough.index)
            expanded.columns = ["symbol", "expiry", "type", "strike"]
            rough = rough.assign(**expanded)

            # Only rows where symbol == exact ticker
            rough = rough[rough["symbol"].str.upper() == ticker.upper()]
            return rough.dropna(subset=["strike"])

        pre_sub = filter_exact(df_pre)
        post_sub = filter_exact(df_post)
        if pre_sub.empty or post_sub.empty:
            continue

        pre_atm = pick_atm(pre_sub)
        post_atm = pick_atm(post_sub)

        if pre_atm is None or post_atm is None:
            continue

        pre_call = pre_atm[pre_atm["type"] == "call"].iloc[0]
        pre_put  = pre_atm[pre_atm["type"] == "put"].iloc[0]
        post_call = post_atm[post_atm["type"] == "call"].iloc[0]
        post_put  = post_atm[post_atm["type"] == "put"].iloc[0]

        K = pre_call["strike"]
        S_pre = K
        S_post = K
        T = 1/365

        # --- Compute synthetic midpoint price ---
        def mid(row):
            return np.mean([row["open"], row["high"], row["low"], row["close"]])

        price_pre  = np.mean([mid(pre_call),  mid(pre_put)])
        price_post = np.mean([mid(post_call), mid(post_put)])
        price_change = price_post - price_pre

        # --- Compute IVs ---
        iv_pre_call = implied_vol(pre_call["close"], S_pre, K, T, RISK_FREE, True)
        iv_pre_put  = implied_vol(pre_put["close"],  S_pre, K, T, RISK_FREE, False)
        iv_post_call = implied_vol(post_call["close"], S_post, K, T, RISK_FREE, True)
        iv_post_put  = implied_vol(post_put["close"],  S_post, K, T, RISK_FREE, False)

        iv_pre = np.nanmean([iv_pre_call, iv_pre_put])
        iv_post = np.nanmean([iv_post_call, iv_post_put])
        iv_crush = iv_pre - iv_post

        rv30 = compute_rv30(ticker, dt)

        all_rows.append({
            "ticker": ticker,
            "earn_date": earn_dt,
            "pre_date": pre,
            "post_date": post,
            "strike": K,

            # NEW PRICE FIELDS
            "price_pre": price_pre,
            "price_post": price_post,
            "price_change": price_change,

            # IV fields
            "IV_pre_call": iv_pre_call,
            "IV_pre_put": iv_pre_put,
            "IV_post_call": iv_post_call,
            "IV_post_put": iv_post_put,
            "IV_pre": iv_pre,
            "IV_post": iv_post,
            "IV_crush": iv_crush,
            "RV30": rv30,
        })


    return pd.DataFrame(all_rows)



# ================================================================
# MASTER BUILDER
# ================================================================
if __name__ == "__main__":
    master_rows = []

    for f in sorted(os.listdir(EARNINGS_DIR)):
        if f.endswith("_earnings.csv"):
            tkr = f.split("_")[0]
            print(f"🔍 Processing {tkr}")
            df = process_ticker(tkr)
            master_rows.append(df)

    if master_rows:
        master_df = pd.concat(master_rows, ignore_index=True)
        master_df.to_csv(MASTER_OUT, index=False)
        print(f"\n✅ MASTER FILE SAVED:\n{MASTER_OUT}")
    else:
        print("\n⚠️ No valid data found.")
