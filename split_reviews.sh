#!/bin/bash

grep '"overall": 1.0\|"overall": 2.0' sample_reviews.json > low_reviews.json
grep '"overall": 4.0\|"overall": 5.0' sample_reviews.json > high_reviews.json

echo "Low reviews:"
wc -l low_reviews.json

echo "High reviews:"
wc -l high_reviews.json
