#!/bin/bash
set -e

# ===============================================================
# 🧩 Full IV Crush Pipeline Runner
# ---------------------------------------------------------------
# Usage:
#   ./run_iv_crush_pipeline.sh AAPL
#
# Steps:
#   1️⃣ Fetch earnings data for TICKER
#   2️⃣ Get options tickers from Massive REST API
#   3️⃣ Download pre/post option flatfiles
#   4️⃣ Run IV crush analysis
# ===============================================================

# --- Check argument ---
if [ -z "$1" ]; then
  echo "❌ Usage: $0 <TICKER>"
  exit 1
fi

TICKER=$(echo "$1" | tr '[:lower:]' '[:upper:]')

# --- Directory setup ---
ROOT_DIR="$(pwd)"
EARNINGS_DIR="${ROOT_DIR}/earnings_data"
TICKER_FILE="${EARNINGS_DIR}/${TICKER}_earnings.csv"
FLATFILES_DIR="${ROOT_DIR}/polygon_flat_files/us_options_opra"

# --- Step 1: Get Earnings Data ---
echo "=============================================================="
echo "📅 Step 1: Fetching earnings data for ${TICKER}"
echo "=============================================================="
python3 get_earnings_data.py "$TICKER"

if [ ! -f "$TICKER_FILE" ]; then
  echo "❌ Earnings file not found for ${TICKER} at ${TICKER_FILE}"
  exit 1
fi

# --- Step 2: Get Option Tickers ---
echo ""
echo "=============================================================="
echo "🔍 Step 2: Fetching historical option tickers for ${TICKER}"
echo "=============================================================="
python3 get_options_ticker_from_earnings.py "$TICKER"

# --- Step 3: Download Flatfiles ---
echo ""
echo "=============================================================="
echo "💾 Step 3: Downloading pre/post-earnings flatfiles"
echo "=============================================================="
bash flat_file_download.sh

# --- Step 3.5: Unzip flatfiles ---
echo ""
echo "🗜️  Unzipping downloaded flatfiles..."
find "$FLATFILES_DIR" -type f -name "*.gz" | while read -r f; do
  echo "   📂 Extracting $(basename "$f")"
  gunzip -f "$f"
done

# --- Step 4: Run IV Crush Analysis ---
echo ""
echo "=============================================================="
echo "📈 Step 4: Running IV Crush analysis for ${TICKER}"
echo "=============================================================="
python3 iv_crush_analysis.py

echo ""
echo "✅ Pipeline complete for ${TICKER}"
echo "   → Check 'options_data/${TICKER}_iv_crush.csv'"


# --- Step 5: Cleanup temporary flatfiles ---
echo ""
echo "🧹 Cleaning up temporary flatfiles for ${TICKER}..."
rm -f "${FLATFILES_DIR}"/*.csv

# --- Done ---
echo ""
echo "✅ Pipeline complete for ${TICKER}"
echo "   → Results: ${OUTPUT_DIR}/${TICKER}_iv_crush.csv"
echo "   → Temporary flatfiles removed."