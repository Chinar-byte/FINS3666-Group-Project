#!/bin/bash
set -e

# ===============================================================
# 🌍 IV Crush Cached Pipeline (persistent tmp folder)
# ---------------------------------------------------------------
# Keeps ±2 day flatfiles and reuses cached downloads.
# Each ticker shares one persistent temp_unzipped directory.
# ===============================================================

ENDPOINT="https://files.massive.com"
FLAT_DIR="polygon_flat_files/us_options_opra"
TMP_DIR="${FLAT_DIR}/tmp_unzipped"
OUT_DIR="options_data"
MASTER_CSV="${OUT_DIR}/atm_iv_earnings_master_2.csv"
EARNINGS_DIR="earnings_data"

mkdir -p "$FLAT_DIR" "$OUT_DIR" "$TMP_DIR"

aws configure set aws_access_key_id "f9062fb8-454d-4f3d-a598-3622caee4bfd"
aws configure set aws_secret_access_key "aQM_P6K_4kZjebUDUXQsC3nednoTpYRH"

TICKERS=(
  AAPL MSFT AMZN GOOGL META NVDA TSLA AMD NFLX CRM ORCL INTC
  JPM GS MS BAC C WFC AXP PYPL
  BA CAT DE GE HON UPS FDX LMT RTX
  NKE MCD SBUX COST HD LOW TGT WMT PG KO PEP
  JNJ PFE MRK UNH ABBV LLY
)

echo "=============================================================="
echo "🚀 Running IV-Crush Pipeline (persistent tmp_unzipped)"
echo "=============================================================="

for TICKER in "${TICKERS[@]}"; do
  echo ""
  echo "=============================================================="
  echo "📊 Processing ${TICKER}"
  echo "=============================================================="

  EARNINGS_FILE="${EARNINGS_DIR}/${TICKER}_earnings.csv"
  if [ ! -f "$EARNINGS_FILE" ]; then
    echo "⚠️ Earnings file missing for ${TICKER}, skipping..."
    continue
  fi

  # --- Step 1: Ensure flatfiles for ±2 days ---
  while IFS=, read -r earn_date _; do
    [[ "$earn_date" == "EarningsDate" || -z "$earn_date" ]] && continue
    clean_date=$(echo "$earn_date" | sed 's/ .*//g')

    for offset in -2 -1 1 2; do
      if [[ "$OSTYPE" == "darwin"* ]]; then
        date_str=$(date -v${offset}d -jf "%Y-%m-%d" "$clean_date" +"%Y-%m-%d" 2>/dev/null || true)
      else
        date_str=$(date -d "$clean_date ${offset} day" +"%Y-%m-%d" 2>/dev/null || true)
      fi
      [ -z "$date_str" ] && continue

      FILE_PATH="us_options_opra/day_aggs_v1/${date_str:0:4}/${date_str:5:2}/${date_str}.csv.gz"
      DEST_PATH="${FLAT_DIR}/${date_str}.csv.gz"

      if [[ -f "$DEST_PATH" ]]; then
        echo "   ✅ Cached ${date_str}"
      else
        echo "   📥 Downloading ${FILE_PATH} ..."
        aws s3 cp "s3://flatfiles/${FILE_PATH}" "$DEST_PATH" \
          --endpoint-url "$ENDPOINT" || echo "   ⚠️ Missing flatfile for ${date_str}"
      fi
    done
  done < "$EARNINGS_FILE"

  # --- Step 2: Extract to shared tmp folder ---
  echo "📂 Extracting flatfiles to shared tmp_unzipped..."
  rm -f ${TMP_DIR}/*.csv 2>/dev/null || true

  for gz in ${FLAT_DIR}/*.csv.gz; do
    [ -f "$gz" ] || continue
    base=$(basename "$gz" .csv.gz)
    gunzip -c "$gz" > "${TMP_DIR}/${base}.csv"
  done

  # --- Step 3: Run analysis ---
  echo "📈 Running analysis for ${TICKER}..."
  python3 build_master_iv_crush_file.py "$TICKER" "$TMP_DIR"

  LOCAL_OUT="${OUT_DIR}/atm_iv_earnings_flatfiles.csv"
  if [ -f "$LOCAL_OUT" ]; then
    echo "🧩 Appending ${LOCAL_OUT} → ${MASTER_CSV}"
    if [ ! -f "$MASTER_CSV" ]; then
      cp "$LOCAL_OUT" "$MASTER_CSV"
    else
      tail -n +2 "$LOCAL_OUT" >> "$MASTER_CSV"
    fi
  fi

  echo "✅ Done with ${TICKER}"
done

echo ""
echo "=============================================================="
echo "🏁 All tickers processed!"
echo "→ Master CSV: ${MASTER_CSV}"
echo "=============================================================="
