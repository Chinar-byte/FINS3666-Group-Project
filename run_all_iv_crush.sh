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
MASTER_CSV="${OUT_DIR}/atm_iv_earnings_master_3.csv"
EARNINGS_DIR="earnings_data"

mkdir -p "$FLAT_DIR" "$OUT_DIR" "$TMP_DIR"

aws configure set aws_access_key_id "f9062fb8-454d-4f3d-a598-3622caee4bfd"
aws configure set aws_secret_access_key "aQM_P6K_4kZjebUDUXQsC3nednoTpYRH"

TICKERS=(
  ############################################
  # TECHNOLOGY
  ############################################
  AAPL MSFT GOOGL GOOG AMZN META NVDA AVGO
  CRM ADBE CSCO ORCL TXN QCOM AMD IBM INTC
  AMAT INTU MU ANET NOW KLAC LRCX SNPS CDNS
  PANW FTNT BKNG NFLX ADP PAYX MSI HPQ WDC
  ZBRA GLW AKAM ENPH FSLR MCHP MPWR SWKS
  QRVO TSM EBAY ETSY

  ############################################
  # FINANCIALS
  ############################################
  JPM BAC WFC C GS MS BLK SCHW CME ICE SPGI
  MCO CB PGR TRV AIG AON MET PRU ALL AFL BK
  STT TFC USB RF MTB HBAN FITB CMA KEY COF
  DFS PNC V MA PYPL AXP BRK.B TROW AMP BEN
  IVZ NTRS

  ############################################
  # AUTOMOTIVE & TRANSPORT
  ############################################
  TSLA UBER
  CSX NSC UNP
  LUV DAL AAL UAL
  UPS FDX

  ############################################
  # CONSUMER DISCRETIONARY / RETAIL
  ############################################
  HD MCD NKE SBUX TJX LOW TGT ROST DG DLTR
  MAR HLT YUM CMG CCL RCL NCLH AAP AZO ORLY
  ULTA LEN PHM DHI NVR TOL POOL ETSY EBAY

  ############################################
  # CONSUMER STAPLES
  ############################################
  PG KO PEP WMT COST MDLZ MO PM KMB CL CLX
  TAP STZ GIS K CPB SJM KR WBA HSY CHD BG ADM
  BALL

  ############################################
  # INDUSTRIALS
  ############################################
  CAT DE GE HON MMM ITW EMR ETN PH ROK GWW
  JCI CARR OTIS MAS J IR NDSN FAST SNA CMI
  PCAR URI ALLE AME AOS EXPD

  ############################################
  # ENERGY
  ############################################
  XOM CVX COP OXY EOG PXD DVN MRO APA HES
  VLO MPC PSX HAL SLB BKR

  ############################################
  # MATERIALS
  ############################################
  LIN APD ECL SHW NUE NEM FCX MOS CF ALB
  MLM VMC PKG IP WRK

  ############################################
  # TELECOM
  ############################################
  T VZ TMUS

  ############################################
  # UTILITIES
  ############################################
  NEE DUK SO SRE EXC AEP XEL PEG ED D WEC
  EIX PNW AEE CMS
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
