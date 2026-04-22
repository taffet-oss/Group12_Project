#!/usr/bin/env python3
"""Train logistic regression and random forest models from the full review file.

This script streams the Amazon review JSON-lines file, skips malformed records
and 3-star reviews, then labels 1/2-star reviews as bad and 4/5-star reviews
as good. To keep the run feasible on a laptop, it uses binary indicators for
the most common full-corpus words and compresses identical feature patterns
with sample weights before fitting the models.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import sys
import time
from collections import defaultdict
from pathlib import Path

import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression


TOKEN_RE = re.compile(r"[a-z]+")
CONTRACTION_STEMS = {
    "don": "dont",
    "didn": "didnt",
    "doesn": "doesnt",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Train full-data review sentiment models from Amazon JSON lines."
    )
    parser.add_argument(
        "--input",
        default="../All_Amazon_Review.json",
        help="Path to the full Amazon review JSON-lines file.",
    )
    parser.add_argument(
        "--counts",
        default="results/word_counts_by_group.csv",
        help="Full-corpus word counts created by analyze_reviews.sh.",
    )
    parser.add_argument(
        "--outdir",
        default="results/full_models",
        help="Directory where model summaries will be written.",
    )
    parser.add_argument(
        "--vocab-size",
        type=int,
        default=100,
        help="Number of top full-corpus words to use as model features.",
    )
    parser.add_argument(
        "--test-fraction",
        type=float,
        default=0.2,
        help="Deterministic fraction of labeled reviews held out for testing.",
    )
    parser.add_argument(
        "--rf-trees",
        type=int,
        default=100,
        help="Number of trees for the random forest.",
    )
    parser.add_argument(
        "--rf-min-samples-leaf",
        type=int,
        default=25,
        help="Minimum weighted samples per random forest leaf.",
    )
    parser.add_argument(
        "--jobs",
        type=int,
        default=-1,
        help="Parallel jobs for random forest; -1 uses all available cores.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=405,
        help="Random seed for model training.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Optional limit on complete JSON records for testing. 0 means no limit.",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=500000,
        help="Print progress after this many complete records.",
    )
    return parser.parse_args()


def load_vocabulary(counts_path: Path, vocab_size: int) -> list[str]:
    totals: dict[str, int] = defaultdict(int)

    with counts_path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if row["group"] in {"low", "high"}:
                totals[row["word"]] += int(row["count"])

    if not totals:
        raise ValueError(f"No low/high word counts found in {counts_path}")

    return [
        word
        for word, _count in sorted(
            totals.items(), key=lambda item: (-item[1], item[0])
        )[:vocab_size]
    ]


def stable_test_split(record: dict, doc_index: int, test_fraction: float) -> bool:
    key = "|".join(
        [
            str(record.get("reviewerID", "")),
            str(record.get("asin", "")),
            str(record.get("unixReviewTime", "")),
            str(doc_index),
        ]
    )
    digest = hashlib.blake2b(key.encode("utf-8"), digest_size=8).digest()
    value = int.from_bytes(digest, byteorder="big") / 2**64
    return value < test_fraction


def feature_mask(text: str, word_to_bit: dict[str, int]) -> int:
    mask = 0
    normalized = text.lower().replace("'", "").replace("`", "")
    for match in TOKEN_RE.finditer(normalized):
        word = CONTRACTION_STEMS.get(match.group(0), match.group(0))
        bit = word_to_bit.get(word)
        if bit is not None:
            mask |= 1 << bit
    return mask


def increment(counts: dict[int, list[int]], mask: int, label: int) -> None:
    current = counts.get(mask)
    if current is None:
        current = [0, 0]
        counts[mask] = current
    current[label] += 1


def stream_and_aggregate(
    input_path: Path,
    word_to_bit: dict[str, int],
    test_fraction: float,
    limit: int,
    progress_every: int,
) -> tuple[dict[int, list[int]], dict[int, list[int]], dict[str, int]]:
    train_counts: dict[int, list[int]] = {}
    test_counts: dict[int, list[int]] = {}
    stats = defaultdict(int)
    started = time.time()

    with input_path.open(encoding="utf-8", errors="replace") as handle:
        for line_number, line in enumerate(handle, start=1):
            line = line.strip()
            if not line:
                continue

            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                stats["malformed_lines"] += 1
                continue

            stats["complete_records"] += 1
            if limit and stats["complete_records"] > limit:
                stats["complete_records"] -= 1
                break

            rating = int(float(record.get("overall", 0)))
            if rating == 3:
                stats["neutral_reviews_skipped"] += 1
                continue
            if rating in {1, 2}:
                label = 1
                stats["bad_reviews"] += 1
            elif rating in {4, 5}:
                label = 0
                stats["good_reviews"] += 1
            else:
                stats["other_ratings_skipped"] += 1
                continue

            text = " ".join(
                [
                    str(record.get("reviewText") or ""),
                    str(record.get("summary") or ""),
                ]
            )
            mask = feature_mask(text, word_to_bit)
            if stable_test_split(record, stats["complete_records"], test_fraction):
                increment(test_counts, mask, label)
                stats["test_reviews"] += 1
            else:
                increment(train_counts, mask, label)
                stats["train_reviews"] += 1

            if progress_every and stats["complete_records"] % progress_every == 0:
                elapsed = time.time() - started
                print(
                    f"Processed {stats['complete_records']:,} complete records "
                    f"({stats['good_reviews'] + stats['bad_reviews']:,} labeled) "
                    f"in {elapsed / 60:.1f} min",
                    flush=True,
                )

    stats["unique_train_patterns"] = len(train_counts)
    stats["unique_test_patterns"] = len(test_counts)
    stats["labeled_reviews"] = stats["good_reviews"] + stats["bad_reviews"]
    return train_counts, test_counts, dict(stats)


def counts_to_arrays(
    counts: dict[int, list[int]], vocab_size: int
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    row_count = sum(1 for values in counts.values() for value in values if value)
    x = np.zeros((row_count, vocab_size), dtype=np.uint8)
    y = np.zeros(row_count, dtype=np.uint8)
    weights = np.zeros(row_count, dtype=np.float64)

    row = 0
    for mask, values in counts.items():
        for label, count in enumerate(values):
            if count == 0:
                continue
            for bit in range(vocab_size):
                x[row, bit] = (mask >> bit) & 1
            y[row] = label
            weights[row] = count
            row += 1

    return x, y, weights


def weighted_confusion(
    actual: np.ndarray, predicted: np.ndarray, weights: np.ndarray
) -> dict[str, float]:
    tn = weights[(actual == 0) & (predicted == 0)].sum()
    fp = weights[(actual == 0) & (predicted == 1)].sum()
    fn = weights[(actual == 1) & (predicted == 0)].sum()
    tp = weights[(actual == 1) & (predicted == 1)].sum()
    total = tn + fp + fn + tp
    return {
        "tn": float(tn),
        "fp": float(fp),
        "fn": float(fn),
        "tp": float(tp),
        "accuracy": float((tn + tp) / total) if total else float("nan"),
    }


def write_csv(path: Path, fieldnames: list[str], rows: list[dict]) -> None:
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    args = parse_args()
    input_path = Path(args.input)
    counts_path = Path(args.counts)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    if not input_path.exists():
        print(f"Input file not found: {input_path}", file=sys.stderr)
        return 1
    if not counts_path.exists():
        print(f"Word count file not found: {counts_path}", file=sys.stderr)
        return 1

    vocabulary = load_vocabulary(counts_path, args.vocab_size)
    word_to_bit = {word: bit for bit, word in enumerate(vocabulary)}

    write_csv(
        outdir / "full_model_vocabulary.csv",
        ["feature_index", "word"],
        [
            {"feature_index": index + 1, "word": word}
            for index, word in enumerate(vocabulary)
        ],
    )

    print(f"Using {len(vocabulary)} word features.")
    print(f"Streaming reviews from {input_path}")

    train_counts, test_counts, stats = stream_and_aggregate(
        input_path=input_path,
        word_to_bit=word_to_bit,
        test_fraction=args.test_fraction,
        limit=args.limit,
        progress_every=args.progress_every,
    )

    write_csv(
        outdir / "full_model_data_summary.csv",
        ["metric", "value"],
        [{"metric": key, "value": value} for key, value in sorted(stats.items())],
    )

    print("Converting compressed feature patterns to model arrays.")
    x_train, y_train, w_train = counts_to_arrays(train_counts, len(vocabulary))
    x_test, y_test, w_test = counts_to_arrays(test_counts, len(vocabulary))

    print(
        f"Train rows after compression: {len(y_train):,}; "
        f"test rows after compression: {len(y_test):,}"
    )

    print("Training logistic regression.")
    logistic = LogisticRegression(
        max_iter=1000,
        solver="lbfgs",
        random_state=args.seed,
    )
    logistic.fit(x_train, y_train, sample_weight=w_train)
    logistic_pred = logistic.predict(x_test)
    logistic_metrics = weighted_confusion(y_test, logistic_pred, w_test)

    print("Training weighted random forest.")
    forest = RandomForestClassifier(
        n_estimators=args.rf_trees,
        min_samples_leaf=args.rf_min_samples_leaf,
        max_features="sqrt",
        bootstrap=False,
        n_jobs=args.jobs,
        random_state=args.seed,
    )
    forest.fit(x_train, y_train, sample_weight=w_train)
    forest_pred = forest.predict(x_test)
    forest_metrics = weighted_confusion(y_test, forest_pred, w_test)

    metrics_rows = []
    for model_name, values in [
        ("logistic_regression", logistic_metrics),
        ("random_forest", forest_metrics),
    ]:
        metrics_rows.append(
            {
                "model": model_name,
                "accuracy": values["accuracy"],
                "actual_good_pred_good": int(values["tn"]),
                "actual_good_pred_bad": int(values["fp"]),
                "actual_bad_pred_good": int(values["fn"]),
                "actual_bad_pred_bad": int(values["tp"]),
            }
        )

    write_csv(
        outdir / "full_model_metrics.csv",
        [
            "model",
            "accuracy",
            "actual_good_pred_good",
            "actual_good_pred_bad",
            "actual_bad_pred_good",
            "actual_bad_pred_bad",
        ],
        metrics_rows,
    )

    coef_rows = [
        {
            "word": word,
            "coefficient_for_bad_review": coef,
        }
        for word, coef in sorted(
            zip(vocabulary, logistic.coef_[0]), key=lambda item: item[1]
        )
    ]
    write_csv(
        outdir / "logistic_coefficients.csv",
        ["word", "coefficient_for_bad_review"],
        coef_rows,
    )

    importance_rows = [
        {"word": word, "importance": importance}
        for word, importance in sorted(
            zip(vocabulary, forest.feature_importances_),
            key=lambda item: item[1],
            reverse=True,
        )
    ]
    write_csv(
        outdir / "random_forest_importance.csv",
        ["word", "importance"],
        importance_rows,
    )

    print("Done. Model metrics:")
    for row in metrics_rows:
        print(
            f"{row['model']}: accuracy={float(row['accuracy']):.4f}, "
            f"[[good->good {row['actual_good_pred_good']}, "
            f"good->bad {row['actual_good_pred_bad']}], "
            f"[bad->good {row['actual_bad_pred_good']}, "
            f"bad->bad {row['actual_bad_pred_bad']}]]"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
