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
MASTER_OUT    = os.path.join(OUTPUT_DIR, "master_iv_rv_crush_with_price_change_2.csv")

RISK_FREE = 0.05
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ================================================================
# Black–Scholes
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
    fn = bs_call if is_call else bs_put
    try:
        return brentq(lambda sig: fn(S, K, T, r, sig) - price, 1e-6, 5.0)
    except:
        return np.nan

# ================================================================
# Parse OPRA
# ================================================================
def parse_opra(opra):
    try:
        t = opra.split(":")[1]
        for i in range(len(t)):
            if t[i:i+6].isdigit():
                symbol = t[:i]
                expiry = "20" + t[i:i+6]
                expiry = datetime.strptime(expiry, "%Y%m%d").strftime("%Y-%m-%d")
                cp = t[i+6]
                strike = int(t[i+7:]) / 1000
                return symbol, expiry, "call" if cp == "C" else "put", strike
    except:
        pass
    return None, None, None, None

# ================================================================
# Load flatfile (NO CACHE)
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
# Yang–Zhang 30-day RV
# ================================================================
def yz_rv(open_, high_, low_, close_):
    if len(close_) < 2:
        return np.nan

    log_ho = np.log(high_ / open_)
    log_lo = np.log(low_ / open_)
    log_oc = np.log(close_ / open_)

    r_overnight = np.log(open_[1:] / close_[:-1])
    r_close = np.log(close_[1:] / close_[:-1])

    k = 0.34 / (1.34 + (len(close_) + 1)/(len(close_) - 1))

    var_overnight = np.var(r_overnight, ddof=1)
    var_oc = np.var(log_oc, ddof=1)
    var_cc = np.var(r_close, ddof=1)
    var_rs = np.mean(log_ho * log_lo)

    yz = var_overnight + k*var_oc + (1-k)*var_cc - var_rs
    return np.sqrt(max(yz, 0))

def compute_rv30(ticker, dt):
    rows = []
    for offset in range(1, 30):
        day = (dt - timedelta(days=offset)).strftime("%Y-%m-%d")
        df = load_flatfile(day)
        if df is None:
            continue
        sub = df[df["ticker"].str.contains(f"O:{ticker}", na=False)]
        if sub.empty:
            continue
        rows.append([
            sub["open"].median(),
            sub["high"].median(),
            sub["low"].median(),
            sub["close"].median(),
        ])
    if len(rows) < 10:
        return np.nan
    arr = np.array(rows)
    return yz_rv(arr[:,0], arr[:,1], arr[:,2], arr[:,3])

# ================================================================
# ATM Picker
# ================================================================
def pick_atm(df):
    m = df["strike"].median()
    filtered = df[(df["strike"] > 0.5*m) & (df["strike"] < 1.5*m)].copy()
    if filtered.empty:
        return None
    filtered["strike_dist"] = (filtered["strike"] - m).abs()
    atm = filtered.sort_values("strike_dist").groupby("type").head(1)
    return atm if len(atm) == 2 else None

# ================================================================
# Process one ticker (single-threaded)
# ================================================================
def process_ticker(ticker):
    earnings_csv = os.path.join(EARNINGS_DIR, f"{ticker}_earnings.csv")
    if not os.path.exists(earnings_csv):
        return pd.DataFrame()

    df_earn = pd.read_csv(earnings_csv)
    rows = []

    for _, r in df_earn.iterrows():
        earn_dt = pd.to_datetime(r["EarningsDate"]).date()
        dt = datetime.strptime(str(earn_dt), "%Y-%m-%d")

        pre  = (dt - timedelta(days=1)).strftime("%Y-%m-%d")
        post = (dt + timedelta(days=1)).strftime("%Y-%m-%d")

        df_pre  = load_flatfile(pre)
        df_post = load_flatfile(post)
        if df_pre is None or df_post is None:
            continue

        def clean(df):
            sub = df[df["ticker"].str.contains(f"O:{ticker}", na=False)].copy()
            if sub.empty:
                return sub
            parsed = sub["ticker"].apply(parse_opra)
            expanded = pd.DataFrame(parsed.tolist(), index=sub.index,
                                    columns=["symbol","expiry","type","strike"])
            sub = sub.join(expanded)
            return sub[sub["symbol"].str.upper() == ticker.upper()].dropna(subset=["strike"])

        pre_sub = clean(df_pre)
        post_sub = clean(df_post)
        if pre_sub.empty or post_sub.empty:
            continue

        pre_atm = pick_atm(pre_sub)
        post_atm = pick_atm(post_sub)
        if pre_atm is None or post_atm is None:
            continue

        pre_call = pre_atm[pre_atm["type"]=="call"].iloc[0]
        pre_put  = pre_atm[pre_atm["type"]=="put"].iloc[0]
        post_call = post_atm[post_atm["type"]=="call"].iloc[0]
        post_put  = post_atm[post_atm["type"]=="put"].iloc[0]

        K = pre_call["strike"]
        T = 1/365

        iv_pre_call  = implied_vol(pre_call["close"],  K, K, T, RISK_FREE, True)
        iv_post_call = implied_vol(post_call["close"], K, K, T, RISK_FREE, True)
        iv_pre_put   = implied_vol(pre_put["close"],   K, K, T, RISK_FREE, False)
        iv_post_put  = implied_vol(post_put["close"],  K, K, T, RISK_FREE, False)

        rv30 = compute_rv30(ticker, dt)

        # CALL
        rows.append({
            "ticker": ticker,
            "earn_date": earn_dt,
            "type": "call",
            "strike": K,
            "pre_date": pre,
            "post_date": post,
            "price_pre": pre_call["close"],
            "price_post": post_call["close"],
            "price_change": post_call["close"] - pre_call["close"],
            "IV_pre": iv_pre_call,
            "IV_post": iv_post_call,
            "IV_crush": iv_pre_call - iv_post_call,
            "RV30": rv30,
        })

        # PUT
        rows.append({
            "ticker": ticker,
            "earn_date": earn_dt,
            "type": "put",
            "strike": K,
            "pre_date": pre,
            "post_date": post,
            "price_pre": pre_put["close"],
            "price_post": post_put["close"],
            "price_change": post_put["close"] - pre_put["close"],
            "IV_pre": iv_pre_put,
            "IV_post": iv_post_put,
            "IV_crush": iv_pre_put - iv_post_put,
            "RV30": rv30,
        })

    return pd.DataFrame(rows)

# ================================================================
# MASTER BUILDER
# ================================================================
if __name__ == "__main__":
    frames = []

    for f in sorted(os.listdir(EARNINGS_DIR)):
        if f.endswith("_earnings.csv"):
            ticker = f.split("_")[0]
            print("Processing", ticker)
            frames.append(process_ticker(ticker))

    if frames:
        master_df = pd.concat(frames, ignore_index=True)
        master_df.to_csv(MASTER_OUT, index=False)
        print("\nSaved:", MASTER_OUT)
    else:
        print("No data.")
