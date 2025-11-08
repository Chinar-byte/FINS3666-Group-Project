#!/bin/bash
set -e

aws configure set aws_access_key_id "f9062fb8-454d-4f3d-a598-3622caee4bfd"
aws configure set aws_secret_access_key "aQM_P6K_4kZjebUDUXQsC3nednoTpYRH"

ENDPOINT="https://files.massive.com"
EARNINGS_DIR="earnings_data"
DEST_DIR="polygon_flat_files/us_options_opra"


mkdir -p "$DEST_DIR"

echo "=============================================="
echo "📦 Downloading Polygon Options Flatfiles"
echo "=============================================="

# --- Loop through all earnings CSVs ---
for file in "$EARNINGS_DIR"/*.csv; do
  SYMBOL=$(basename "$file" | cut -d'_' -f1 | tr '[:lower:]' '[:upper:]')
  echo ""
  echo "🔍 $SYMBOL - Reading earnings dates from $file"

  # Extract clean YYYY-MM-DD only (ignore times or timezones)
  RAW_DATES=$(awk -F',' 'NR>1 {print $1}' "$file" | sed 's/ .*//g' | sort -u)

  # Iterate over each date
  echo "$RAW_DATES" | while read -r d; do
    [ -z "$d" ] && continue

    # For each earnings date, get pre (−1d), earnings day, and post (+1d)
    for offset in -1 0 1; do
      if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS date command
        date_str=$(date -v${offset}d -jf "%Y-%m-%d" "$d" +"%Y-%m-%d" 2>/dev/null || true)
      else
        # Linux date command
        date_str=$(date -d "$d ${offset} day" +"%Y-%m-%d" 2>/dev/null || true)
      fi

      [ -z "$date_str" ] && continue

      year=$(echo "$date_str" | cut -d'-' -f1)
      month=$(echo "$date_str" | cut -d'-' -f2)
      FILE_PATH="us_options_opra/day_aggs_v1/${year}/${month}/${date_str}.csv.gz"
      DEST_PATH="${DEST_DIR}/${date_str}.csv.gz"

      if [[ -f "$DEST_PATH" ]]; then
        echo "   ✅ Already downloaded ${FILE_PATH}"
        continue
      fi

      echo "   📥 Downloading ${FILE_PATH} ..."
      aws s3 cp "s3://flatfiles/${FILE_PATH}" "$DEST_PATH" \
        --endpoint-url "$ENDPOINT" || echo "   ⚠️ Missing flatfile for ${date_str}"
    done
  done
done

echo ""
echo "✅ All available option flatfiles downloaded to $DEST_DIR"