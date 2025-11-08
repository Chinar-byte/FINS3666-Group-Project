#!/bin/bash
set -e

# ==============================================================
# 📦 Massive Flatfile Downloader — Pre/Post Earnings Option Data
# ==============================================================
# Downloads only the flatfiles for (earn_date - 1 day)
# and (earn_date + 1 day) for each earnings CSV in earnings_data/.
#
# Usage:
#   ./flat_file_download.sh
# ==============================================================

# --- CONFIG ---
AWS_ACCESS_KEY_ID="f9062fb8-454d-4f3d-a598-3622caee4bfd"
AWS_SECRET_ACCESS_KEY="aQM_P6K_4kZjebUDUXQsC3nednoTpYRH"
ENDPOINT="https://files.massive.com"
EARNINGS_DIR="earnings_data"
DEST_DIR="polygon_flat_files/us_options_opra"

# --- Setup AWS keys locally ---
aws configure set aws_access_key_id "$AWS_ACCESS_KEY_ID"
aws configure set aws_secret_access_key "$AWS_SECRET_ACCESS_KEY"

mkdir -p "$DEST_DIR"

echo "============================================================="
echo "📡 Starting Massive Flatfile Downloader"
echo "============================================================="

# --- Loop through all earnings CSVs ---
for file in "$EARNINGS_DIR"/*.csv; do
  SYMBOL=$(basename "$file" | cut -d'_' -f1 | tr '[:lower:]' '[:upper:]')
  echo ""
  echo "🔍 $SYMBOL - Reading earnings dates from $file"

  # Extract clean YYYY-MM-DD dates (ignore timestamps/timezones)
  RAW_DATES=$(awk -F',' 'NR>1 {print $1}' "$file" | sed 's/ .*//g' | sort -u)

  while read -r d; do
    [ -z "$d" ] && continue

    # Compute PRE (−1d) and POST (+1d)
    if [[ "$OSTYPE" == "darwin"* ]]; then
      PRE_DATE=$(date -v-1d -jf "%Y-%m-%d" "$d" +"%Y-%m-%d" 2>/dev/null || true)
      POST_DATE=$(date -v+1d -jf "%Y-%m-%d" "$d" +"%Y-%m-%d" 2>/dev/null || true)
    else
      PRE_DATE=$(date -d "$d -1 day" +"%Y-%m-%d" 2>/dev/null || true)
      POST_DATE=$(date -d "$d +1 day" +"%Y-%m-%d" 2>/dev/null || true)
    fi

    # Skip invalid dates
    [ -z "$PRE_DATE" ] && continue
    [ -z "$POST_DATE" ] && continue

    for TARGET_DATE in "$PRE_DATE" "$POST_DATE"; do
      YEAR=$(echo "$TARGET_DATE" | cut -d'-' -f1)
      MONTH=$(echo "$TARGET_DATE" | cut -d'-' -f2)
      FILE_PATH="us_options_opra/day_aggs_v1/${YEAR}/${MONTH}/${TARGET_DATE}.csv.gz"
      DEST_PATH="${DEST_DIR}/${TARGET_DATE}.csv.gz"

      if [[ -f "$DEST_PATH" ]]; then
        echo "   ✅ Already exists: ${TARGET_DATE}"
        continue
      fi

      echo "   📥 Downloading flatfile for ${TARGET_DATE}..."
      aws s3 cp "s3://flatfiles/${FILE_PATH}" "$DEST_PATH" \
        --endpoint-url "$ENDPOINT" || echo "   ⚠️ Missing flatfile for ${TARGET_DATE}"
    done
  done <<< "$RAW_DATES"
done

echo ""
echo "✅ All available pre/post-earnings flatfiles downloaded to $DEST_DIR"
