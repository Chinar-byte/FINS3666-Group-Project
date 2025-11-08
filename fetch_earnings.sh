#!/bin/bash
set -e

# ===============================================================
# 📅 BULK EARNINGS FETCHER
# ---------------------------------------------------------------
# Runs get_earnings_data.py for all 45 tickers.
# Skips existing CSVs in earnings_data/.
#
# Usage:
#   ./fetch_all_earnings.sh
# ===============================================================

EARNINGS_DIR="earnings_data"
SCRIPT="get_earnings_data.py"

mkdir -p "$EARNINGS_DIR"

# --- Define the 45 tickers ---
TICKERS=(
  # Tech
  AAPL MSFT AMZN GOOGL META NVDA TSLA AMD NFLX CRM ORCL INTC
  # Financials
  JPM GS MS BAC C WFC AXP PYPL
  # Industrials
  BA CAT DE GE HON UPS FDX LMT RTX
  # Consumer
  NKE MCD SBUX COST HD LOW TGT WMT PG KO PEP
  # Pharma
  JNJ PFE MRK UNH ABBV LLY
)

echo "=============================================================="
echo "🚀 Fetching earnings data for ${#TICKERS[@]} tickers"
echo "=============================================================="

counter=1
for TICKER in "${TICKERS[@]}"; do
  echo ""
  echo "[$counter/${#TICKERS[@]}] ======================================"
  echo "📊 Fetching earnings data for ${TICKER}"
  echo "======================================="

  OUTFILE="${EARNINGS_DIR}/${TICKER}_earnings.csv"

  if [ -f "$OUTFILE" ]; then
    echo "✅ ${TICKER}: Earnings file already exists → skipping"
  else
    echo "⏳ Running python3 ${SCRIPT} ${TICKER}"
    python3 "${SCRIPT}" "${TICKER}" || echo "⚠️ ${TICKER}: Failed to fetch earnings data"
  fi

  ((counter++))
done

echo ""
echo "=============================================================="
echo "🏁 All tickers processed. Check 'earnings_data/' directory."
echo "=============================================================="
