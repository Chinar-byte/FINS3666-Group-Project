#!/bin/bash
set -e

# ===============================================================
# 💾 Enhanced Flatfile Downloader (±2 days around earnings)
# ---------------------------------------------------------------
# Usage:
#   bash flat_file_download_plusminus2.sh
#
# Downloads compressed option flatfiles from Massive S3, only for
# 2 days before and after each earnings date in earnings_data/.
# Keeps cached .gz files; skips duplicates.
# ===============================================================

ENDPOINT="https://files.massive.com"
EARNINGS_DIR="earnings_data"
DEST_DIR="polygon_flat_files/us_options_opra"

mkdir -p "$DEST_DIR"

# --- AWS creds (already known) ---
aws configure set aws_access_key_id "f9062fb8-454d-4f3d-a598-3622caee4bfd"
aws configure set aws_secret_access_key "aQM_P6K_4kZjebUDUXQsC3nednoTpYRH"

echo "=============================================================="
echo "📦 Downloading options flatfiles (±2 days around earnings)"
echo "=============================================================="

# --- Iterate through each earnings CSV ---
for file in "$EARNINGS_DIR"/*.csv; do
  SYMBOL=$(basename "$file" | cut -d'_' -f1 | tr '[:lower:]' '[:upper:]')
  echo ""
  echo "🔍 ${SYMBOL} — reading earnings dates from $file"

  # Read dates from the first column, skip header, strip timezones
  RAW_DATES=$(awk -F',' 'NR>1 {print $1}' "$file" | sed 's/ .*//g' | sort -u)

  echo "$RAW_DATES" | while read -r d; do
    [ -z "$d" ] && continue

    # ------------------------------------------------------------
    # Download 30 days BEFORE + 1 and 2 days AFTER earnings date
    # ------------------------------------------------------------

    # Days we want:
    #   -30 to -1   (30-day RV window)
    #   +1, +2       (post-earnings comparison)
    # ------------------------------------------------------------
    for offset in $(seq -30 -1) 1 2; do

      # Compute date
      if [[ "$OSTYPE" == "darwin"* ]]; then
        date_str=$(date -v"${offset}"d -jf "%Y-%m-%d" "$d" +"%Y-%m-%d" 2>/dev/null || true)
      else
        date_str=$(date -d "$d ${offset} day" +"%Y-%m-%d" 2>/dev/null || true)
      fi

      # Skip invalid dates
      [[ -z "$date_str" ]] && continue

      year=$(echo "$date_str" | cut -d'-' -f1)
      month=$(echo "$date_str" | cut -d'-' -f2)

      FILE_PATH="us_options_opra/day_aggs_v1/${year}/${month}/${date_str}.csv.gz"
      DEST_PATH_UNZIPPED="${DEST_DIR}/${date_str}.csv"
      DEST_PATH="${DEST_DIR}/${date_str}.csv.gz"


      # Skip if already downloaded
      if [[ -f "$DEST_PATH"  || -f "$DEST_PATH_UNZIPPED" ]]; then
        echo "   ✅ Already have ${date_str}"
        continue
      fi

      # Download
      echo "   📥 Downloading ${FILE_PATH} ..."
      aws s3 cp "s3://flatfiles/${FILE_PATH}" "$DEST_PATH" \
        --endpoint-url "$ENDPOINT" \
        || echo "   ⚠️ Missing: ${date_str} (skipping)"

    done

  done
done

echo ""
echo "✅ All available option flatfiles (±2 days) downloaded to $DEST_DIR"
