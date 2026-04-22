#!/bin/bash
set -euo pipefail

INPUT="${1:-../All_Amazon_Review.json}"
OUTDIR="${2:-results}"
TOP_N="${TOP_N:-20}"

STOP_WORDS="the to i it and a this is my of t s for you that with but have in on be can me she so was as they are not we he her him his at if or from all an has had them were your when will would could get got just really also very its it's i'm i've you're don't doesn't didn't"

if [ ! -f "$INPUT" ]; then
  echo "Could not find input file: $INPUT" >&2
  echo "Usage: $0 [path/to/reviews.json] [output_directory]" >&2
  exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "This script needs jq, but jq was not found." >&2
  exit 1
fi

mkdir -p "$OUTDIR"

echo "Analyzing reviews from: $INPUT"
echo "Writing results to: $OUTDIR"

LC_ALL=C perl -ne '
  if (/^\s*\{.*\}\s*$/) {
    print;
  } else {
    $skipped++;
  }
  END {
    warn "Skipped $skipped malformed line(s).\n" if $skipped;
  }
' "$INPUT" |
jq -r '[.overall, (.reviewText // "" | gsub("[\r\n\t]"; " "))] | @tsv' |
awk -v outdir="$OUTDIR" -v stop_words="$STOP_WORDS" '
BEGIN {
  split(stop_words, stop_word_list, " ")
  for (i in stop_word_list) {
    stop_words_seen[stop_word_list[i]] = 1
  }
}
{
  rating = int($1)
  total_reviews++
  rating_counts[rating]++

  text = $0
  sub(/^[^\t]*\t/, "", text)
  text = tolower(text)
  gsub(/\047/, "", text)
  gsub(/`/, "", text)
  gsub(/[^a-z]+/, " ", text)

  n_words = split(text, words, " ")
  for (i = 1; i <= n_words; i++) {
    word = words[i]
    if (word == "don") {
      word = "dont"
    } else if (word == "didn") {
      word = "didnt"
    } else if (word == "doesn") {
      word = "doesnt"
    }
    if (word == "" || word in stop_words_seen) {
      continue
    }

    word_counts["all" SUBSEP word]++

    if (rating == 1 || rating == 2) {
      word_counts["low" SUBSEP word]++
    } else if (rating == 4 || rating == 5) {
      word_counts["high" SUBSEP word]++
    }
  }
}
END {
  total_file = outdir "/total_reviews.txt"
  ratings_file = outdir "/rating_counts.csv"
  words_file = outdir "/word_counts_by_group.csv"

  print total_reviews > total_file

  print "rating,count" > ratings_file
  for (rating = 1; rating <= 5; rating++) {
    print rating "," rating_counts[rating] + 0 >> ratings_file
  }

  print "group,word,count" > words_file
  for (key in word_counts) {
    split(key, parts, SUBSEP)
    print parts[1] "," parts[2] "," word_counts[key] >> words_file
  }
}
'

awk -F, 'NR > 1 && $1 == "all" { print $3, $2 }' "$OUTDIR/word_counts_by_group.csv" |
  LC_ALL=C sort -nr |
  sed -n "1,${TOP_N}p" > "$OUTDIR/top_all_words.txt"

awk -F, 'NR > 1 && $1 == "low" { print $3, $2 }' "$OUTDIR/word_counts_by_group.csv" |
  LC_ALL=C sort -nr |
  sed -n "1,${TOP_N}p" > "$OUTDIR/top_low_words.txt"

awk -F, 'NR > 1 && $1 == "high" { print $3, $2 }' "$OUTDIR/word_counts_by_group.csv" |
  LC_ALL=C sort -nr |
  sed -n "1,${TOP_N}p" > "$OUTDIR/top_high_words.txt"

echo "Done."
echo "Total reviews: $(cat "$OUTDIR/total_reviews.txt")"
echo "Rating counts:"
cat "$OUTDIR/rating_counts.csv"
