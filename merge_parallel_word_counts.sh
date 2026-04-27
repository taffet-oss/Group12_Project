#!/bin/bash
set -euo pipefail

PARTIAL_GLOB="${1:-partial_counts_*.csv}"
TOP_N="${2:-20}"
MERGED_FILE="${3:-word_counts_by_group_parallel.csv}"
LOW_FILE="${4:-top_low_words_parallel.csv}"
HIGH_FILE="${5:-top_high_words_parallel.csv}"

shopt -s nullglob
files=( $PARTIAL_GLOB )

if [ ${#files[@]} -eq 0 ]; then
  echo "No partial count files matched: $PARTIAL_GLOB" >&2
  exit 1
fi

tmp_merged="$(mktemp)"

awk -F, '
BEGIN { OFS = "," }
FNR == 1 { next }
{
  gsub(/\r/, "", $1)
  gsub(/\r/, "", $2)
  gsub(/\r/, "", $3)
  gsub(/"/, "", $1)
  gsub(/"/, "", $2)
  gsub(/"/, "", $3)
  key = $1 SUBSEP $2
  counts[key] += $3
}
END {
  for (key in counts) {
    split(key, parts, SUBSEP)
    print parts[1], parts[2], counts[key]
  }
}
' "${files[@]}" |
  sort -t, -k1,1 -k3,3nr -k2,2 > "$tmp_merged"

{
  echo "group,word,count"
  cat "$tmp_merged"
} > "$MERGED_FILE"

rm -f "$tmp_merged"

{
  echo "word,count"
  awk -F, -v top_n="$TOP_N" 'NR > 1 && $1 == "low" && count < top_n { print $2 "," $3; count++ }' "$MERGED_FILE"
} > "$LOW_FILE"

{
  echo "word,count"
  awk -F, -v top_n="$TOP_N" 'NR > 1 && $1 == "high" && count < top_n { print $2 "," $3; count++ }' "$MERGED_FILE"
} > "$HIGH_FILE"

echo "Wrote $MERGED_FILE"
echo "Wrote $LOW_FILE"
echo "Wrote $HIGH_FILE"
