#!/bin/bash
set -euo pipefail

python3 run_full_models.py \
  --input ../All_Amazon_Review.json \
  --counts results/word_counts_by_group.csv \
  --outdir results/full_models \
  "$@"
