# import os
# import pandas as pd
# import numpy as np
# from scipy.stats import norm
# from scipy.optimize import brentq
# from datetime import datetime, timedelta

# # === CONFIG ===
# EARNINGS_DIR = "earnings_data"
# FLATFILES_DIR = "polygon_flat_files/us_options_opra"
# OUTPUT_DIR = "options_data"
# RISK_FREE_RATE = 0.05

# os.makedirs(OUTPUT_DIR, exist_ok=True)

# # === Black-Scholes helpers ===
# def black_scholes_call(S, K, T, r, sigma):
#     if T <= 0 or sigma <= 0:
#         return 0
#     d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
#     d2 = d1 - sigma * np.sqrt(T)
#     return S * norm.cdf(d1) - K * np.exp(-r * T) * norm.cdf(d2)

# def black_scholes_put(S, K, T, r, sigma):
#     if T <= 0 or sigma <= 0:
#         return 0
#     d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
#     d2 = d1 - sigma * np.sqrt(T)
#     return K * np.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)

# def implied_vol(price, S, K, T, r, opt_type='call'):
#     intrinsic = max(S - K, 0) if opt_type == 'call' else max(K - S, 0)
#     if price <= 0 or price < intrinsic:
#         return None
#     try:
#         fn = black_scholes_call if opt_type == 'call' else black_scholes_put
#         return brentq(lambda x: fn(S, K, T, r, x) - price, 0.001, 5.0)
#     except:
#         return None

# # === Parse option ticker ===
# def parse_option_ticker(ticker):
#     try:
#         t = ticker.split(":")[1]
#         # Example: O:AAPL241220C00190000 → symbol=AAPL, expiry=2024-12-20, type=call, strike=190.0
#         for i in range(len(t)):
#             if t[i:i+6].isdigit():
#                 symbol = t[:i]
#                 expiry_raw = t[i:i+6]
#                 expiry = datetime.strptime("20" + expiry_raw, "%Y%m%d").strftime("%Y-%m-%d")
#                 opt_type = "call" if t[i+6] == "C" else "put"
#                 strike = int(t[i+7:]) / 1000.0
#                 return symbol, expiry, opt_type, strike
#     except:
#         return None, None, None, None
#     return None, None, None, None

# # === Load flatfile ===
# def load_flatfile(path):
#     try:
#         return pd.read_csv(path, compression="infer")
#     except Exception as e:
#         print(f"⚠️ Error loading {path}: {e}")
#         return None

# # === Collect available flatfile dates ===
# flatfile_dates = []
# for f in os.listdir(FLATFILES_DIR):
#     name = os.path.splitext(f)[0]
#     try:
#         d = datetime.strptime(name, "%Y-%m-%d")
#         flatfile_dates.append(d)
#     except:
#         continue

# flatfile_dates = sorted(flatfile_dates)
# print(f"📂 Found {len(flatfile_dates)} flatfiles in {FLATFILES_DIR}")
# if flatfile_dates:
#     print(f"   → Earliest: {flatfile_dates[0].date()}, Latest: {flatfile_dates[-1].date()}")

# # === Main Loop ===
# for file in os.listdir(EARNINGS_DIR):
#     if not file.endswith(".csv"):
#         continue

#     symbol = file.split("_")[0].upper()
#     earnings_path = os.path.join(EARNINGS_DIR, file)
#     df_earnings = pd.read_csv(earnings_path)

#     print(f"\n{'='*70}\nAnalyzing {symbol}\n{'='*70}")

#     records = []

#     for _, row in df_earnings.iterrows():
#         try:
#             # Convert aware → naive to allow comparison
#             earn_date = pd.to_datetime(str(row["EarningsDate"]), utc=False).tz_localize(None)
#         except Exception as e:
#             print(f"⚠️ Could not parse {row.get('EarningsDate')}: {e}")
#             continue

#         # Convert all flatfile dates to naive
#         flatfile_naive = [d.replace(tzinfo=None) for d in flatfile_dates]

#         # Find nearest flatfiles before and after earnings
#         pre_date = max([d for d in flatfile_naive if d <= earn_date], default=None)
#         post_date = min([d for d in flatfile_naive if d >= earn_date], default=None)

#         if not pre_date or not post_date:
#             print(f"⚠️ Skipping {earn_date.date()} — no nearby flatfiles found")
#             continue

#         pre_file = os.path.join(FLATFILES_DIR, pre_date.strftime("%Y-%m-%d") + ".csv")
#         post_file = os.path.join(FLATFILES_DIR, post_date.strftime("%Y-%m-%d") + ".csv")

#         df_pre = load_flatfile(pre_file)
#         df_post = load_flatfile(post_file)
#         if df_pre is None or df_post is None:
#             continue

#         # Filter only this symbol's options
#         df_pre_symbol = df_pre[df_pre["ticker"].astype(str).str.startswith(f"O:{symbol}")]
#         df_post_symbol = df_post[df_post["ticker"].astype(str).str.startswith(f"O:{symbol}")]

#         if df_pre_symbol.empty or df_post_symbol.empty:
#             print(f"⚠️ No {symbol} options found around {earn_date.date()}")
#             continue

#         for _, opt in df_pre_symbol.iterrows():
#             ticker = opt["ticker"]
#             price_pre = opt["close"]
#             post_match = df_post_symbol[df_post_symbol["ticker"] == ticker]

#             if post_match.empty:
#                 continue

#             price_post = float(post_match.iloc[0]["close"])
#             sym, expiry, opt_type, strike = parse_option_ticker(ticker)
#             if not expiry:
#                 continue

#             # Approximate stock as ATM
#             S = strike
#             expiry_date = datetime.strptime(expiry, "%Y-%m-%d")
#             T_pre = (expiry_date - pre_date).days / 365
#             T_post = (expiry_date - post_date).days / 365

#             iv_pre = implied_vol(price_pre, S, strike, T_pre, RISK_FREE_RATE, opt_type)
#             iv_post = implied_vol(price_post, S, strike, T_post, RISK_FREE_RATE, opt_type)
#             iv_crush = (iv_pre - iv_post) * 100 if iv_pre and iv_post else None

#             records.append({
#                 "symbol": symbol,
#                 "earn_date": earn_date.date(),
#                 "ticker": ticker,
#                 "as_of_pre": pre_date.date(),
#                 "as_of_post": post_date.date(),
#                 "price_pre": price_pre,
#                 "price_post": price_post,
#                 "price_change": price_post - price_pre,
#                 "implied_vol_pre": iv_pre,
#                 "implied_vol_post": iv_post,
#                 "iv_crush": iv_crush,
#                 "expiry": expiry,
#                 "strike": strike,
#                 "type": opt_type,
#                 "pre_file": os.path.basename(pre_file),
#                 "post_file": os.path.basename(post_file)
#             })

#     # Save per-symbol output
#     if records:
#         out_path = os.path.join(OUTPUT_DIR, f"{symbol}_iv_crush.csv")
#         pd.DataFrame(records).to_csv(out_path, index=False)
#         print(f"✅ Saved {out_path} ({len(records)} rows)")
#     else:
#         print(f"⚠️ No valid records for {symbol}")

# print("\n🎉 IV Crush analysis complete.")

"""
ATM IV + IV30 + RV30 Earnings Analysis
--------------------------------------
Fixed version:
• Removes deprecated `from arch import volatility`
• Implements Yang–Zhang RV manually (pure Python)
• Keeps your Black–Scholes, parsing, flatfile logic identical
• Works with Python 3.13–3.14 and arch 8.0
"""

import os
import pandas as pd
import numpy as np
from scipy.stats import norm
from scipy.optimize import brentq
from datetime import datetime, timedelta

# ============================================================
# CONFIG
# ============================================================
EARNINGS_DIR = "earnings_data"
OPTION_DIR = "polygon_flat_files/us_options_opra"
UNDERLYING_DIR = "underlying_flatfiles"      # adjust if needed
OUTPUT_DIR = "options_data"
RISK_FREE_RATE = 0.05

os.makedirs(OUTPUT_DIR, exist_ok=True)


# ============================================================
# Black–Scholes functions
# ============================================================
def black_scholes_call(S, K, T, r, sigma):
    if T <= 0 or sigma <= 0:
        return 0
    d1 = (np.log(S/K)+(r+0.5*sigma**2)*T)/(sigma*np.sqrt(T))
    d2 = d1 - sigma*np.sqrt(T)
    return S*norm.cdf(d1) - K*np.exp(-r*T)*norm.cdf(d2)

def black_scholes_put(S, K, T, r, sigma):
    if T <= 0 or sigma <= 0:
        return 0
    d1 = (np.log(S/K)+(r+0.5*sigma**2)*T)/(sigma*np.sqrt(T))
    d2 = d1 - sigma*np.sqrt(T)
    return K*np.exp(-r*T)*norm.cdf(-d2) - S*norm.cdf(-d1)

def implied_vol(price, S, K, T, r, opt_type):
    intrinsic = max(S-K, 0) if opt_type=="call" else max(K-S, 0)
    if price <= intrinsic or price <= 0 or T <= 0:
        return None
    fn = black_scholes_call if opt_type=="call" else black_scholes_put
    try:
        return brentq(lambda x: fn(S,K,T,r,x)-price, 1e-4, 5.0)
    except:
        return None


# ============================================================
# Parse OPRA option ticker
# ============================================================
def parse_option_ticker(ticker):
    try:
        t = ticker.split(":")[1]
        for i in range(len(t)):
            if t[i:i+6].isdigit():
                symbol = t[:i]
                expiry = datetime.strptime("20"+t[i:i+6], "%Y%m%d")
                opt_type = "call" if t[i+6]=="C" else "put"
                strike = int(t[i+7:])/1000.0
                return symbol, expiry, opt_type, strike
    except:
        return None, None, None, None
    return None, None, None, None


# ============================================================
# Load flatfile
# ============================================================
def load_flatfile(path):
    try:
        return pd.read_csv(path, compression="infer")
    except Exception as e:
        print(f"⚠️ Error loading {path}: {e}")
        return None


# ============================================================
# Pure Python Yang–Zhang Realised Volatility (RV30)
# ============================================================
def compute_rv_yang_zhang(opens, highs, lows, closes):
    """
    Yang-Zhang estimator implemented manually.
    Returns daily volatility.
    """
    O = np.log(opens.values)
    H = np.log(highs.values)
    L = np.log(lows.values)
    C = np.log(closes.values)

    # overnight
    oc = O[1:] - C[:-1]
    # open-high / open-low / close-open
    rs = (H - O)*(H - C) + (L - O)*(L - C)

    # close-close
    cc = C[1:] - C[:-1]

    n = len(C)
    if n < 2:
        return None

    k = 0.34 / (1.34 + (n + 1)/(n - 1))

    sigma_o = np.var(oc, ddof=1)
    sigma_c = np.var(cc, ddof=1)
    sigma_rs = np.mean(rs)

    return np.sqrt(k * sigma_o + (1 - k) * sigma_c + sigma_rs)


def compute_rv30(symbol, earn_date):
    rows = []
    for off in range(1, 31+1):
        d = earn_date - timedelta(days=off)
        f = os.path.join(UNDERLYING_DIR, symbol, d.strftime("%Y-%m-%d") + ".csv")
        if not os.path.exists(f):
            continue
        try:
            df = pd.read_csv(f)
            if not {"open","high","low","close"}.issubset(df.columns):
                continue
            rows.append(df.loc[0, ["open","high","low","close"]])
        except:
            continue

    if len(rows) < 5:
        return None

    df = pd.DataFrame(rows)

    daily = compute_rv_yang_zhang(df["open"], df["high"], df["low"], df["close"])
    if daily is None:
        return None

    return float(daily * np.sqrt(252))  # annualize


# ============================================================
# IV30 calculation (expiry interpolation)
# ============================================================
def compute_iv30(df_pre_symbol, S, pre_date):
    points = []
    for _, row in df_pre_symbol.iterrows():
        tkr = row["ticker"]
        _, expiry, opt_type, strike = parse_option_ticker(tkr)
        T = (expiry - pre_date).days / 365
        if T <= 0:
            continue

        price = row["close"]
        iv = implied_vol(price, S, strike, T, RISK_FREE_RATE, opt_type)
        if iv is not None:
            points.append((T, iv))

    if len(points) < 2:
        return None

    df = pd.DataFrame(points, columns=["T","IV"]).sort_values("T")
    target_T = 30 / 365

    before = df[df["T"] <= target_T].tail(1)
    after  = df[df["T"] >= target_T].head(1)
    if before.empty or after.empty:
        return None

    T1, IV1 = before.iloc[0]
    T2, IV2 = after.iloc[0]

    if T1 == T2:
        return float(IV1)

    IV30 = IV1 + (IV2 - IV1)*(target_T - T1)/(T2 - T1)
    return float(IV30)


# ============================================================
# Discover flatfile dates
# ============================================================
flatfile_dates = []
for f in os.listdir(OPTION_DIR):
    name = os.path.splitext(f)[0]
    try:
        d = datetime.strptime(name, "%Y-%m-%d.csv.gz")
        flatfile_dates.append(d)
    except:
        continue

flatfile_dates = sorted(flatfile_dates)
print(f"📂 Found {len(flatfile_dates)} option flatfiles.")
if flatfile_dates:
    print(f"   Earliest: {flatfile_dates[0].date()}, Latest: {flatfile_dates[-1].date()}")


# ============================================================
# MAIN LOOP
# ============================================================
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
            earn_date = pd.to_datetime(str(row["EarningsDate"]), utc=False).tz_localize(None)
        except:
            continue
        
        # print(f"🔍 Processing earnings date: {earn_date.date()}")

        # nearest flatfiles
        flatfile_naive = [d.replace(tzinfo=None) for d in flatfile_dates]
        pre_date  = max([d for d in flatfile_naive if d <= earn_date], default=None)
        post_date = min([d for d in flatfile_naive if d >= earn_date], default=None)
        
        # print(flatfile_naive)

        if not pre_date or not post_date:
            continue

        pre_file  = os.path.join(OPTION_DIR, pre_date.strftime("%Y-%m-%d") + ".csv")
        post_file = os.path.join(OPTION_DIR, post_date.strftime("%Y-%m-%d") + ".csv")

        df_pre  = load_flatfile(pre_file)
        df_post = load_flatfile(post_file)
        if df_pre is None or df_post is None:
            continue

        # this symbol only
        df_pre_symbol  = df_pre[df_pre["ticker"].astype(str).str.startswith(f"O:{symbol}")].copy()
        df_post_symbol = df_post[df_post["ticker"].astype(str).str.startswith(f"O:{symbol}")].copy()
        if df_pre_symbol.empty or df_post_symbol.empty:
            continue

        df_pre_symbol["strike"] = df_pre_symbol["ticker"].apply(lambda t: parse_option_ticker(t)[3])
        S = df_pre_symbol["strike"].median()

        df_pre_symbol["atm_dist"] = (df_pre_symbol["strike"] - S).abs()
        atm_df = df_pre_symbol[df_pre_symbol["atm_dist"] == df_pre_symbol["atm_dist"].min()].copy()

        iv30 = compute_iv30(df_pre_symbol, S, pre_date)
        rv30 = compute_rv30(symbol, earn_date)

        for _, opt in atm_df.iterrows():
            tkr = opt["ticker"]
            _, expiry_dt, opt_type, strike = parse_option_ticker(tkr)

            post_match = df_post_symbol[df_post_symbol["ticker"] == tkr]
            if post_match.empty:
                continue

            price_pre  = opt["close"]
            price_post = float(post_match.iloc[0]["close"])

            T_pre  = (expiry_dt - pre_date).days / 365
            T_post = (expiry_dt - post_date).days / 365

            iv_pre  = implied_vol(price_pre,  S, strike, T_pre,  RISK_FREE_RATE, opt_type)
            iv_post = implied_vol(price_post, S, strike, T_post, RISK_FREE_RATE, opt_type)

            iv_crush = (iv_pre - iv_post)*100 if iv_pre and iv_post else None

            records.append({
                "symbol": symbol,
                "earn_date": earn_date.date(),
                "as_of_pre": pre_date.date(),
                "as_of_post": post_date.date(),
                "ticker": tkr,
                "type": opt_type,
                "strike": strike,
                "expiry": expiry_dt.date(),
                "S": S,

                "price_pre": price_pre,
                "price_post": price_post,
                "price_change": price_post - price_pre,

                "iv_pre": iv_pre,
                "iv_post": iv_post,
                "iv_crush": iv_crush,

                "iv30": iv30,
                "rv30": rv30,

                "pre_file": os.path.basename(pre_file),
                "post_file": os.path.basename(post_file)
            })

    if records:
        out_path = os.path.join(OUTPUT_DIR, f"{symbol}_atm_iv_iv30_rv30.csv")
        pd.DataFrame(records).to_csv(out_path, index=False)
        print(f"✅ Saved {out_path} ({len(records)} rows)")
    else:
        print(f"⚠️ No valid records for {symbol}")

print("\n🎉 Complete — ATM IV + IV30 + RV30 for all symbols.")
