from massive import RESTClient
from datetime import datetime, timedelta
from dateutil import parser
import pandas as pd
import os

API_KEY = "aQM_P6K_4kZjebUDUXQsC3nednoTpYRH"
client = RESTClient(API_KEY)

EARNINGS_DIR = "earnings_data"
OUTPUT_DIR = "options_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# === PARAMETERS ===
DTE_TARGET = 45
DTE_WINDOW = 10  # ±10 days around 45 DTE

def fetch_option_trade_price(ticker, date_str):
    """Fetch last trade price for an option contract on a given date."""
    try:
        trades = list(client.list_trades(
            ticker,
            params={
                "date": date_str,
                "limit": 1,
                "order": "desc",
            },
        ))
        if trades and hasattr(trades[0], "price"):
            return trades[0].price
    except Exception as e:
        print(f"⚠️ Trade fetch failed for {ticker} on {date_str}: {e}")
    return None


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

        # === PRE and POST earnings windows ===
        for label, target_dt in {
            "pre": dt - timedelta(days=1),
            "post": dt + timedelta(days=1),
        }.items():
            target_str = target_dt.strftime("%Y-%m-%d")
            start_exp = (target_dt + timedelta(days=DTE_TARGET - DTE_WINDOW)).strftime("%Y-%m-%d")
            end_exp = (target_dt + timedelta(days=DTE_TARGET + DTE_WINDOW)).strftime("%Y-%m-%d")

            print(f"  {label.upper()} snapshot: {target_str} → expirations {start_exp} to {end_exp}")

            try:
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
            
            print(contracts)

            if not contracts:
                print(f"  ⚠️ No contracts found for {symbol} on {target_str}")
                continue

            # Build local DataFrame for this snapshot
            contracts_data = []
            for c in contracts:
                try:
                    contracts_data.append({
                        "ticker": c.ticker,
                        "strike": c.strike_price,
                        "expiry": c.expiration_date,
                        "contract_type": c.contract_type,
                        "exchange": getattr(c, "primary_exchange", None),
                    })
                except Exception as e:
                    print(f"    ⚠️ Error parsing contract: {e}")

            if not contracts_data:
                continue

            df = pd.DataFrame(contracts_data)

            # === Fetch trade prices for all ===
            prices = []
            for _, opt in df.iterrows():
                trade_price = fetch_option_trade_price(opt["ticker"], target_str)
                prices.append(trade_price)
                print(trade_price)

            df["trade_price"] = prices
            df["context"] = label
            df["symbol"] = symbol
            df["earn_date"] = earn_date_raw
            df["as_of"] = target_str
            
            print(results)

            results.extend(df.to_dict("records"))

    # === Combine PRE & POST, compute differences ===
    df_all = pd.DataFrame(results)
    if df_all.empty:
        print(f"⚠️ No valid data collected for {symbol}.")
        continue

    # Pivot pre/post to compute premium change
    df_pre = df_all[df_all["context"] == "pre"].set_index("ticker")
    df_post = df_all[df_all["context"] == "post"].set_index("ticker")

    merged = df_pre.join(
        df_post,
        lsuffix="_pre",
        rsuffix="_post",
        how="inner"
    )

    merged["premium_change"] = merged["trade_price_pre"] - merged["trade_price_post"]

    # === Save ===
    out_path = os.path.join(OUTPUT_DIR, f"{symbol}_45DTE_trades.csv")
    merged.reset_index().to_csv(out_path, index=False)
    print(f"\n✅ Saved {len(merged)} matched pre/post contracts for {symbol} → {out_path}")
