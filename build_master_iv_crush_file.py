#!/usr/bin/env python3
"""
IV-Crush Analysis (±2-day tolerant)
-----------------------------------
Finds closest available flatfiles up to 2 days before/after earnings.
Computes ATM call/put price + IV crush.
"""
import os, sys, pandas as pd, numpy as np
from datetime import datetime, timedelta
from scipy.stats import norm
from scipy.optimize import brentq
from pathlib import Path

TICKER = sys.argv[1] if len(sys.argv) > 1 else "AAPL"
FLATFILES_DIR = "polygon_flat_files/us_options_opra/tmp_unzipped"
EARNINGS_DIR, OUT_DIR = Path("earnings_data"), Path("options_data")
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_PATH = OUT_DIR / "atm_iv_earnings_flatfiles.csv"
RISK_FREE = 0.05

print(f"\n======================================================================")
print(f"📊 {TICKER} — flatfiles in {FLATFILES_DIR}")
print(f"======================================================================")

def bs_call(S,K,T,r,s):
    if T<=0 or s<=0: return 0
    d1=(np.log(S/K)+(r+0.5*s**2)*T)/(s*np.sqrt(T)); d2=d1-s*np.sqrt(T)
    return S*norm.cdf(d1)-K*np.exp(-r*T)*norm.cdf(d2)


def bs_put(S,K,T,r,s):
    if T<=0 or s<=0: return 0
    d1=(np.log(S/K)+(r+0.5*s**2)*T)/(s*np.sqrt(T)); d2=d1-s*np.sqrt(T)
    return K*np.exp(-r*T)*norm.cdf(-d2)-S*norm.cdf(-d1)


def implied_vol(p,S,K,T,r,typ):
    fn=bs_call if typ=="call" else bs_put
    intrinsic=max(S-K,0) if typ=="call" else max(K-S,0)
    if p<=0 or p<intrinsic: return None
    try: return brentq(lambda x: fn(S,K,T,r,x)-p,1e-4,5.0)
    except: return None
    
    
def parse_opt(t):
    try:
        t=t.split(":")[1]
        for i in range(len(t)):
            if t[i:i+6].isdigit():
                s=t[:i]; e=datetime.strptime("20"+t[i:i+6],"%Y%m%d").strftime("%Y-%m-%d")
                ty="call" if t[i+6]=="C" else "put"; k=int(t[i+7:])/1000
                return s,e,ty,k
    except: return None,None,None,None
    return None,None,None,None


def load(date):
    for ext in [".csv",".csv.gz"]:
        p=os.path.join(FLATFILES_DIR,f"{date}{ext}")
        # print(p)
        # print(os.path.exists(p))
        if os.path.exists(p): return pd.read_csv(p,compression="infer")
    return None

# def nearest_flatfile(base_date, direction):
#     for d in range(1,3):   # search 1→2 days away
#         target = base_date + timedelta(days=d*direction)
#         s = target.strftime("%Y-%m-%d")
#         f = load(s)
#         if f is not None:
#             print(f"✅ Found flatfile for {s}")
#             if d>1: print(f"⚠️ Using {s} ({'+' if direction>0 else '-'}{d} days)")
#             return f, s
#     print(f"⚠️ No flatfile within {direction:+} 2 days of {base_date}")
#     return None, None

def nearest_flatfile(base_date, direction):
    """
    Find the nearest available flatfile up to ±2 days from base_date.
    direction = -1 → before earnings
    direction = +1 → after earnings
    """
    assert direction in (-1, 1)
    for d in range(1, 3):  # look 1 or 2 days away
        target_date = base_date + timedelta(days=d * direction)
        date_str = target_date.strftime("%Y-%m-%d")
        df = load(date_str)
        if df is not None:
            if d > 1:
                print(f"⚠️ Using {date_str} ({'+' if direction > 0 else '-'}{d} days from {base_date})")
            else:
                print(f"✅ Found flatfile for {date_str} ({'after' if direction > 0 else 'before'})")
            return df, date_str

    print(f"⚠️ No flatfile found within {2} days {'after' if direction > 0 else 'before'} {base_date}")
    return None, None


records=[]
earnings_file=EARNINGS_DIR/f"{TICKER}_earnings.csv"
if not earnings_file.exists():
    print(f"⚠️ No earnings file for {TICKER}"); sys.exit(0)

df=pd.read_csv(earnings_file)
for _,row in df.iterrows():
    try: ed=pd.to_datetime(row["EarningsDate"]).date()
    except: continue
    pre,pre_date=nearest_flatfile(ed,-1)
    post,post_date=nearest_flatfile(ed,1)
    if pre is None or post is None: continue

    pre=pre[pre["ticker"].astype(str).str.upper().str.startswith(f"O:{TICKER}")]
    post=post[post["ticker"].astype(str).str.upper().str.startswith(f"O:{TICKER}")]
    if pre.empty or post.empty: continue

    parsed=pre["ticker"].apply(parse_opt)
    pre[["symbol","expiry","type","strike"]]=pd.DataFrame(parsed.tolist(),index=pre.index)
    pre.dropna(subset=["strike"],inplace=True)
    atm=np.median(pre["strike"])

    for typ in ["call","put"]:
        side=pre[pre["type"]==typ]
        if side.empty: continue
        idx=side["strike"].sub(atm).abs().idxmin()
        o=side.loc[idx]; tkr=o["ticker"]; strike=float(o["strike"])
        pre_p=float(o["close"]); pm=post[post["ticker"]==tkr]
        if pm.empty: continue
        post_p=float(pm.iloc[0]["close"])
        exp=o["expiry"]
        Tpre=(datetime.strptime(exp,"%Y-%m-%d")-datetime.strptime(pre_date,"%Y-%m-%d")).days/365
        Tpost=(datetime.strptime(exp,"%Y-%m-%d")-datetime.strptime(post_date,"%Y-%m-%d")).days/365
        iv_pre=implied_vol(pre_p,strike,strike,Tpre,RISK_FREE,typ)
        iv_post=implied_vol(post_p,strike,strike,Tpost,RISK_FREE,typ)
        ivc=(iv_pre-iv_post)*100 if iv_pre and iv_post else None
        records.append({
            "symbol":TICKER,"earn_date":ed,"type":typ,"expiry":exp,"strike":strike,
            "ticker":tkr,"price_pre":pre_p,"price_post":post_p,
            "price_change":post_p-pre_p,"iv_pre":iv_pre,"iv_post":iv_post,"iv_crush":ivc,
            "pre_file":pre_date,"post_file":post_date
        })

if records:
    df_out=pd.DataFrame(records).sort_values("earn_date",ascending=False)
    df_out.to_csv(OUT_PATH,index=False)
    print(f"✅ Saved {len(df_out)} rows → {OUT_PATH}")
else:
    print(f"⚠️ No valid data found for {TICKER}")
