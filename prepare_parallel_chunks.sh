#!/bin/bash
set -euo pipefail

INPUT_FILE="${1:-/staging/sacaceres/All_Amazon_Review.json}"
CHUNK_DIR="${2:-/staging/sacaceres/parallel_word_chunks}"
LINES_PER_CHUNK="${3:-250000}"
MANIFEST_FILE="${4:-parallel_chunk_manifest.txt}"

if [ ! -f "$INPUT_FILE" ]; then
  echo "Input file not found: $INPUT_FILE" >&2
  exit 1
fi

mkdir -p "$CHUNK_DIR"

if [ -n "$(find "$CHUNK_DIR" -maxdepth 1 -type f 2>/dev/null | head -n 1)" ]; then
  echo "Chunk directory already contains files: $CHUNK_DIR" >&2
  echo "Choose a new chunk directory or empty this one first." >&2
  exit 1
fi

split -d -a 4 -l "$LINES_PER_CHUNK" "$INPUT_FILE" "$CHUNK_DIR/reviews_chunk_"

find "$CHUNK_DIR" -maxdepth 1 -type f -name 'reviews_chunk_*' -printf '%f\n' |
  LC_ALL=C sort > "$MANIFEST_FILE"

echo "Created chunk files in: $CHUNK_DIR"
echo "Wrote manifest: $MANIFEST_FILE"
echo "Number of chunks: $(wc -l < "$MANIFEST_FILE")"
