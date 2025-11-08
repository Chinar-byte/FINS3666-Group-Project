from massive import RESTClient
from datetime import datetime, timedelta
from dateutil import parser
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
import pandas as pd
import numpy as np
from scipy.stats import norm
from scipy.optimize import brentq
import time
import os
import threading
import json
import signal

API_KEY = "aQM_P6K_4kZjebUDUXQsC3nednoTpYRH"
client = RESTClient(API_KEY)

EARNINGS_DIR = "earnings_data"
OUTPUT_DIR = "options_data"
CACHE_PATH = "trade_cache.json"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# === PARAMETERS ===
DTE_TARGET = 45
DTE_WINDOW = 10
THROTTLE = 0.5
MAX_WORKERS = 3
API_TIMEOUT = 8  # Max seconds for any single API call
FETCH_TIMEOUT = 10  # Max seconds for future.result()
MAX_RETRIES = 1  # Reduced retries to avoid hanging
RISK_FREE_RATE = 0.05
NUM_ATM_STRIKES = 5  # Look at 5 strikes around ATM to find most liquid

# === TIMEOUT EXCEPTION ===
class TimeoutException(Exception):
    pass

def timeout_handler(signum, frame):
    raise TimeoutException("API call timed out")

# === CACHE SETUP ===
cache_lock = threading.Lock()
try:
    with open(CACHE_PATH, "r") as f:
        trade_cache = json.load(f)
    print(f"📦 Loaded {len(trade_cache)} cached entries")
except FileNotFoundError:
    trade_cache = {}
    print("📦 No cache found, starting fresh")

def save_cache():
    with cache_lock:
        with open(CACHE_PATH, "w") as f:
            json.dump(trade_cache, f, indent=2)

def get_from_cache(key: str):
    with cache_lock:
        return trade_cache.get(key)

def save_to_cache(key: str, value):
    with cache_lock:
        trade_cache[key] = value

# Thread-safe rate limiter
rate_limit_lock = threading.Lock()
last_call_time = 0

def rate_limited_sleep():
    global last_call_time
    with rate_limit_lock:
        elapsed = time.time() - last_call_time
        if elapsed < THROTTLE:
            time.sleep(THROTTLE - elapsed)
        last_call_time = time.time()

# === BLACK-SCHOLES IMPLIED VOLATILITY ===
def black_scholes_call(S, K, T, r, sigma):
    if T <= 0 or sigma <= 0:
        return 0
    d1 = (np.log(S/K) + (r + 0.5*sigma**2)*T) / (sigma*np.sqrt(T))
    d2 = d1 - sigma*np.sqrt(T)
    return S*norm.cdf(d1) - K*np.exp(-r*T)*norm.cdf(d2)

def black_scholes_put(S, K, T, r, sigma):
    if T <= 0 or sigma <= 0:
        return 0
    d1 = (np.log(S/K) + (r + 0.5*sigma**2)*T) / (sigma*np.sqrt(T))
    d2 = d1 - sigma*np.sqrt(T)
    return K*np.exp(-r*T)*norm.cdf(-d2) - S*norm.cdf(-d1)

def calculate_implied_vol(price, S, K, T, r, option_type='call'):
    if price <= 0 or S <= 0 or K <= 0 or T <= 0:
        return None
    
    intrinsic = max(S - K, 0) if option_type == 'call' else max(K - S, 0)
    if price < intrinsic * 0.90:
        return None
    
    try:
        if option_type == 'call':
            iv = brentq(
                lambda sigma: black_scholes_call(S, K, T, r, sigma) - price,
                0.001, 5.0, maxiter=50
            )
        else:
            iv = brentq(
                lambda sigma: black_scholes_put(S, K, T, r, sigma) - price,
                0.001, 5.0, maxiter=50
            )
        return iv if 0.001 <= iv <= 5.0 else None
    except (ValueError, RuntimeError):
        return None

# === API CALLS WITH TIMEOUT ===
def safe_api_call(func, *args, **kwargs):
    """Execute API call with hard timeout"""
    signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(API_TIMEOUT)
    try:
        result = func(*args, **kwargs)
        signal.alarm(0)
        return result
    except TimeoutException:
        signal.alarm(0)
        print(f"  ⏱️  API call timed out after {API_TIMEOUT}s")
        return None
    except Exception as e:
        signal.alarm(0)
        print(f"  ❌ API error: {e}")
        return None

# ---------------- FETCH STOCK PRICE ----------------
def fetch_stock_price(symbol: str, date_str: str):
    cache_key = f"stock|{symbol}|{date_str}"
    cached = get_from_cache(cache_key)
    if cached is not None:
        return cached
    
    rate_limited_sleep()
    
    def _fetch():
        return list(client.list_aggs(
            symbol, 1, "day", date_str, date_str,
            params={"adjusted": "true", "sort": "asc", "limit": 1}
        ))
    
    bars = safe_api_call(_fetch)
    if bars and hasattr(bars[0], "close"):
        price = bars[0].close
        save_to_cache(cache_key, price)
        return price
    
    save_to_cache(cache_key, None)
    return None

# ---------------- FETCH OPTION TRADE WITH TIMESTAMP ----------------
def fetch_option_trade_with_time(ticker: str, date_str: str, retry=0):
    """
    Fetch last trade with timestamp to filter out stale trades
    Returns: (price, timestamp_ms) or (None, None)
    """
    cache_key = f"trade|{ticker}|{date_str}"
    cached = get_from_cache(cache_key)
    if cached is not None:
        return (cached.get("price"), cached.get("timestamp"))
    
    rate_limited_sleep()
    
    def _fetch():
        return list(client.list_trades(
            ticker,
            params={"date": date_str, "limit": 1, "order": "desc"}
        ))
    
    trades = safe_api_call(_fetch)
    
    if trades and hasattr(trades[0], "price") and trades[0].price > 0:
        price = trades[0].price
        timestamp = getattr(trades[0], "sip_timestamp", None)
        data = {"price": price, "timestamp": timestamp}
        save_to_cache(cache_key, data)
        return (price, timestamp)
    
    if retry < MAX_RETRIES:
        time.sleep(THROTTLE * 2)
        return fetch_option_trade_with_time(ticker, date_str, retry + 1)
    
    save_to_cache(cache_key, {"price": None, "timestamp": None})
    return (None, None)

# ---------------- FIND MOST LIQUID ATM OPTION ----------------
def find_best_atm_option(contracts, stock_price, date_str, option_type='call'):
    """
    Find the most liquid ATM option by checking trade recency
    Returns: (best_contract, trade_price, trade_timestamp) or (None, None, None)
    """
    # Filter by type
    filtered = [c for c in contracts if c.contract_type == option_type]
    if not filtered:
        return (None, None, None)
    
    # Sort by distance from stock price, get top 5 closest
    filtered.sort(key=lambda c: abs(c.strike_price - stock_price))
    candidates = filtered[:NUM_ATM_STRIKES]
    
    print(f"    🔎 Checking {len(candidates)} {option_type} strikes near ${stock_price:.2f}...")
    
    # Check which has most recent trade
    best_contract = None
    best_price = None
    best_timestamp = 0
    
    for c in candidates:
        price, timestamp = fetch_option_trade_with_time(c.ticker, date_str)
        
        if price and timestamp and timestamp > best_timestamp:
            best_contract = c
            best_price = price
            best_timestamp = timestamp
            print(f"       ${c.strike_price:.2f}: ${price:.2f} ✓")
        elif price:
            print(f"       ${c.strike_price:.2f}: ${price:.2f} (older)")
        else:
            print(f"       ${c.strike_price:.2f}: No trade")
    
    if best_contract:
        print(f"    ✓ Selected {option_type} strike ${best_contract.strike_price:.2f}")
    else:
        print(f"    ⚠️ No valid {option_type} trades found")
    
    return (best_contract, best_price, best_timestamp)

# ---------------- MAIN LOOP ----------------
try:
    for file in os.listdir(EARNINGS_DIR):
        if not file.endswith(".csv"):
            continue

        symbol = file.split("_")[0].upper()
        earnings = pd.read_csv(os.path.join(EARNINGS_DIR, file))
        results = []

        print(f"\n{'='*70}\n{symbol:^70}\n{'='*70}")

        for _, row in earnings.iterrows():
            earn_date_raw = str(row["EarningsDate"])
            try:
                dt = parser.parse(earn_date_raw).replace(tzinfo=None)
            except Exception as e:
                print(f"⚠️ Could not parse date: {earn_date_raw} ({e})")
                continue

            print(f"\n📅 Earnings: {earn_date_raw}")

            pre_dt = dt - timedelta(days=1)
            post_dt = dt + timedelta(days=1)
            
            pre_date_str = pre_dt.strftime("%Y-%m-%d")
            post_date_str = post_dt.strftime("%Y-%m-%d")
            
            # Fetch stock prices
            print(f"  📈 Fetching stock prices...")
            pre_stock_price = fetch_stock_price(symbol, pre_date_str)
            post_stock_price = fetch_stock_price(symbol, post_date_str)
            
            if not pre_stock_price or not post_stock_price:
                print(f"  ⚠️ Missing stock prices, skipping")
                continue
            
            realized_move_pct = ((post_stock_price - pre_stock_price) / pre_stock_price) * 100
            print(f"  📊 Stock: ${pre_stock_price:.2f} → ${post_stock_price:.2f} ({realized_move_pct:+.2f}%)")

            # Get contracts
            start_exp = (pre_dt + timedelta(days=DTE_TARGET - DTE_WINDOW)).strftime("%Y-%m-%d")
            end_exp = (pre_dt + timedelta(days=DTE_TARGET + DTE_WINDOW)).strftime("%Y-%m-%d")
            
            print(f"  🔍 Finding ATM options (45±10 DTE: {start_exp} to {end_exp})")
            
            def _fetch_contracts():
                return list(client.list_options_contracts(
                    symbol,
                    params={
                        "as_of": pre_date_str,
                        "expiration_date.gte": start_exp,
                        "expiration_date.lte": end_exp,
                        "limit": 100,  # Get more contracts to find liquid ones
                        "order": "asc",
                        "sort": "strike_price",
                    },
                ))
            
            contracts = safe_api_call(_fetch_contracts)
            if not contracts:
                print(f"  ⚠️ No contracts found, skipping")
                time.sleep(THROTTLE)
                continue
            
            time.sleep(THROTTLE)

            # Find most liquid ATM options for PRE date
            print(f"  🎯 Finding most liquid ATM options on {pre_date_str}...")
            atm_call, call_price_pre, _ = find_best_atm_option(contracts, pre_stock_price, pre_date_str, 'call')
            atm_put, put_price_pre, _ = find_best_atm_option(contracts, pre_stock_price, pre_date_str, 'put')
            
            if not atm_call or not atm_put or not call_price_pre or not put_price_pre:
                print(f"  ⚠️ Could not find liquid ATM options, skipping")
                continue
            
            # Fetch POST prices for the same contracts
            print(f"  📡 Fetching POST prices on {post_date_str}...")
            call_price_post, _ = fetch_option_trade_with_time(atm_call.ticker, post_date_str)
            put_price_post, _ = fetch_option_trade_with_time(atm_put.ticker, post_date_str)
            
            if not call_price_post or not put_price_post:
                print(f"  ⚠️ Missing POST prices, skipping")
                continue
            
            # Calculate time to expiration
            expiry_dt = datetime.strptime(atm_call.expiration_date, "%Y-%m-%d")
            dte_pre = (expiry_dt - pre_dt).days / 365.0
            dte_post = (expiry_dt - post_dt).days / 365.0
            moneyness = pre_stock_price / atm_call.strike_price
            
            print(f"  ✓ Selected Strike: ${atm_call.strike_price:.2f} (Moneyness: {moneyness:.3f}, DTE: {(expiry_dt-pre_dt).days})")

            # Calculate implied volatilities
            call_iv_pre = calculate_implied_vol(call_price_pre, pre_stock_price, atm_call.strike_price, dte_pre, RISK_FREE_RATE, 'call')
            call_iv_post = calculate_implied_vol(call_price_post, post_stock_price, atm_call.strike_price, dte_post, RISK_FREE_RATE, 'call')
            put_iv_pre = calculate_implied_vol(put_price_pre, pre_stock_price, atm_put.strike_price, dte_pre, RISK_FREE_RATE, 'put')
            put_iv_post = calculate_implied_vol(put_price_post, post_stock_price, atm_put.strike_price, dte_post, RISK_FREE_RATE, 'put')
            
            # Calculate crushes
            call_iv_crush = None
            put_iv_crush = None
            call_premium_change = call_price_pre - call_price_post
            put_premium_change = put_price_pre - put_price_post
            
            if call_iv_pre and call_iv_post:
                call_iv_crush = call_iv_pre - call_iv_post
                print(f"  💥 CALL: IV {call_iv_pre*100:.1f}% → {call_iv_post*100:.1f}% (Crush: {call_iv_crush*100:+.1f} pts)")
                print(f"     Premium: ${call_price_pre:.2f} → ${call_price_post:.2f} (Δ: ${call_premium_change:+.2f})")
            else:
                print(f"  ⚠️ CALL IV: Unable to calculate")
            
            if put_iv_pre and put_iv_post:
                put_iv_crush = put_iv_pre - put_iv_post
                print(f"  💥 PUT:  IV {put_iv_pre*100:.1f}% → {put_iv_post*100:.1f}% (Crush: {put_iv_crush*100:+.1f} pts)")
                print(f"     Premium: ${put_price_pre:.2f} → ${put_price_post:.2f} (Δ: ${put_premium_change:+.2f})")
            else:
                print(f"  ⚠️ PUT IV: Unable to calculate")

            # Store results
            results.append({
                "symbol": symbol,
                "earnings_date": earn_date_raw,
                "pre_date": pre_date_str,
                "post_date": post_date_str,
                "pre_stock_price": pre_stock_price,
                "post_stock_price": post_stock_price,
                "realized_move_pct": realized_move_pct,
                "atm_strike": atm_call.strike_price,
                "moneyness": moneyness,
                "expiry": atm_call.expiration_date,
                "dte": (expiry_dt - pre_dt).days,
                "call_ticker": atm_call.ticker,
                "put_ticker": atm_put.ticker,
                "call_price_pre": call_price_pre,
                "call_price_post": call_price_post,
                "call_premium_change": call_premium_change,
                "call_iv_pre_pct": call_iv_pre * 100 if call_iv_pre else None,
                "call_iv_post_pct": call_iv_post * 100 if call_iv_post else None,
                "call_iv_crush_pct": call_iv_crush * 100 if call_iv_crush else None,
                "put_price_pre": put_price_pre,
                "put_price_post": put_price_post,
                "put_premium_change": put_premium_change,
                "put_iv_pre_pct": put_iv_pre * 100 if put_iv_pre else None,
                "put_iv_post_pct": put_iv_post * 100 if put_iv_post else None,
                "put_iv_crush_pct": put_iv_crush * 100 if put_iv_crush else None,
            })

        # Save results
        if results:
            df = pd.DataFrame(results)
            out_path = os.path.join(OUTPUT_DIR, f"{symbol}_iv_crush_analysis.csv")
            df.to_csv(out_path, index=False)
            print(f"\n✅ Saved {len(df)} earnings events to {out_path}")
            
            # Statistics
            valid_call = df[df["call_iv_crush_pct"].notna()]
            
            if len(valid_call) > 0:
                print(f"\n📊 CALL OPTION STATISTICS:")
                print(f"   Sample Size: {len(valid_call)} events")
                print(f"   Avg Pre-Earnings IV: {valid_call['call_iv_pre_pct'].mean():.1f}%")
                print(f"   Avg Post-Earnings IV: {valid_call['call_iv_post_pct'].mean():.1f}%")
                print(f"   Avg IV Crush: {valid_call['call_iv_crush_pct'].mean():.1f} pts")
                print(f"   Avg Premium Change: ${valid_call['call_premium_change'].mean():.2f}")
                print(f"   Avg Realized Move: {valid_call['realized_move_pct'].abs().mean():.2f}%")
                
                positive_crush = len(valid_call[valid_call['call_iv_crush_pct'] > 0])
                print(f"   IV Crush Rate: {positive_crush}/{len(valid_call)} ({positive_crush/len(valid_call)*100:.0f}%)")

except KeyboardInterrupt:
    print("\n⚠️ Interrupted by user")
finally:
    save_cache()
    print(f"\n💾 Saved cache with {len(trade_cache)} entries")

print("\n🎉 All done!")