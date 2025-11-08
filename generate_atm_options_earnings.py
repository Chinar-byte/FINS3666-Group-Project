import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dateutil import parser
from massive import RESTClient  # requires `pip install massive`
import yfinance as yf

# === CONFIG ===
RISK_FREE_RATE = 0.05
DTE_TARGET = 45
DTE_WINDOW = 10
OUTFILE = "atm_earnings_options.csv"

# === SECTOR GROUPS ===
SECTORS = {
    "Tech": ["AAPL", "MSFT", "AMZN", "GOOGL", "META", "NVDA", "TSLA", "AMD", "NFLX", "CRM", "ORCL", "INTC"],
    "Financials": ["JPM", "GS", "MS", "BAC", "C", "WFC", "AXP", "PYPL"],
    "Industrials": ["BA", "CAT", "DE", "GE", "HON", "UPS", "FDX", "LMT", "RTX"],
    "Consumer": ["NKE", "MCD", "SBUX", "COST", "HD", "LOW", "TGT", "WMT", "PG", "KO", "PEP"],
    "Pharma": ["JNJ", "PFE", "MRK", "UNH", "ABBV", "LLY"]
}

# === Massive API client ===
API_KEY = "aQM_P6K_4kZjebUDUXQsC3nednoTpYRH"
client = RESTClient(API_KEY)

# === Helper: Get earnings dates ===
def get_earnings_dates(symbol):
    try:
        tkr = yf.Ticker(symbol)
        df = tkr.get_earnings_dates(limit=20)
        if "Earnings Date" in df.columns:
            df.rename(columns={"Earnings Date": "EarningsDate"}, inplace=True)
        df.reset_index(inplace=True)
        return pd.to_datetime(df["EarningsDate"]).dt.date.tolist()
    except Exception as e:
        print(f"⚠️ {symbol}: could not fetch earnings dates ({e})")
        return []

# === Helper: Find ATM option tickers ===
def get_atm_contracts(symbol, date):
    """Use Massive API to get closest-to-money call & put contracts for 45±10 DTE."""
    try:
        start_exp = (date + timedelta(days=DTE_TARGET - DTE_WINDOW)).strftime("%Y-%m-%d")
        end_exp = (date + timedelta(days=DTE_TARGET + DTE_WINDOW)).strftime("%Y-%m-%d")

        contracts = list(client.list_options_contracts(
            symbol,
            params={
                "as_of": date.strftime("%Y-%m-%d"),
                "expiration_date.gte": start_exp,
                "expiration_date.lte": end_exp,
                "limit": 100,
                "order": "asc",
                "sort": "strike_price"
            },
        ))

        if not contracts:
            return None, None

        # Get current stock price (for ATM selection)
        price = yf.Ticker(symbol).history(period="5d")["Close"].iloc[-1]

        df = pd.DataFrame([{
            "ticker": c.ticker,
            "strike": c.strike_price,
            "expiry": c.expiration_date,
            "type": "call" if "C" in c.ticker else "put"
        } for c in contracts])

        df["diff"] = abs(df["strike"] - price)
        df = df.sort_values("diff")

        atm_call = df[df["type"] == "call"].head(1)
        atm_put = df[df["type"] == "put"].head(1)

        return (
            atm_call.iloc[0].to_dict() if not atm_call.empty else None,
            atm_put.iloc[0].to_dict() if not atm_put.empty else None
        )
    except Exception as e:
        print(f"⚠️ Error fetching ATM contracts for {symbol} ({e})")
        return None, None

# === Main Loop ===
records = []

for sector, tickers in SECTORS.items():
    for symbol in tickers:
        print(f"\n{'='*70}\n{sector} → {symbol}\n{'='*70}")

        earnings_dates = get_earnings_dates(symbol)
        if not earnings_dates:
            continue

        for e_date in earnings_dates:
            earn_date = datetime.combine(e_date, datetime.min.time())
            pre_date = earn_date - timedelta(days=1)
            post_date = earn_date + timedelta(days=1)

            print(f"📅 {symbol} Earnings: {e_date} | Pre: {pre_date.date()} | Post: {post_date.date()}")

            call_pre, put_pre = get_atm_contracts(symbol, pre_date)
            call_post, put_post = get_atm_contracts(symbol, post_date)

            for opt_type, pre, post in [("call", call_pre, call_post), ("put", put_pre, put_post)]:
                if not pre or not post:
                    continue

                # Get close price for each (Massive trades/quotes endpoint)
                try:
                    pre_price = client.get_option_aggregate(pre["ticker"], params={"as_of": pre_date.strftime("%Y-%m-%d")}).close
                    post_price = client.get_option_aggregate(post["ticker"], params={"as_of": post_date.strftime("%Y-%m-%d")}).close
                except Exception:
                    pre_price, post_price = None, None

                records.append({
                    "sector": sector,
                    "symbol": symbol,
                    "earn_date": e_date,
                    "type": opt_type,
                    "expiry": pre["expiry"],
                    "strike": pre["strike"],
                    "ticker": pre["ticker"],
                    "price_pre": pre_price,
                    "price_post": post_price,
                    "price_change": (post_price - pre_price) if (pre_price and post_price) else None
                })

# === Save all results ===
df_out = pd.DataFrame(records)
os.makedirs("options_data", exist_ok=True)
out_path = os.path.join("options_data", OUTFILE)
df_out.to_csv(out_path, index=False)
print(f"\n✅ Saved combined ATM option prices → {out_path}")
