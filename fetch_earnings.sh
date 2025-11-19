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

# # --- Define the 45 tickers ---
# TICKERS=(
#   # Tech
#   AAPL MSFT AMZN GOOGL META NVDA TSLA AMD NFLX CRM ORCL INTC
#   # Financials
#   JPM GS MS BAC C WFC AXP PYPL
#   # Industrials
#   BA CAT DE GE HON UPS FDX LMT RTX
#   # Consumer
#   NKE MCD SBUX COST HD LOW TGT WMT PG KO PEP
#   # Pharma
#   JNJ PFE MRK UNH ABBV LLY
# )

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
