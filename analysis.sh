#!/bin/bash

echo "Starting Group 12 R Analysis at $(date)"

if [ -f /staging/taffet/amazon-review-dataset.zip ]; then
    echo "Unzipping data from staging..."
    unzip -o /staging/taffet/amazon-review-dataset.zip -d .
fi

echo "Running Rscript analysis.R..."
Rscript analysis.R

if [ -f "rating_analysis_results.csv" ]; then
    echo "Success! Results generated."
else
    echo "Error: Results file not found."
fi
