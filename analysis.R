# Group 12: Amazon Review Language Pattern Analysis
# This script processes the full dataset to find word patterns in ratings.

# Load necessary libraries (Make sure these are in your CHTC container)
library(jsonlite)
library(dplyr)
library(stringr)

# 1. Load the data 
# Adjust the path to where your teammate uploads the file
input_path <- "/staging/taffet/Amazon_Reviews.json" 

message("Reading dataset from: ", input_path)

# Using stream_in because the file is likely too big for standard read_json
data <- stream_in(file(input_path))

# 2. Basic Data Cleaning
# Focus on the 'overall' rating and the 'reviewText'
cleaned_data <- data %>%
  select(overall, reviewText) %>%
  filter(!is.na(reviewText))

# 3. Analyze Word Counts vs. Ratings
# Hypothesis: Are 1-star reviews longer than 5-star reviews?
analysis <- cleaned_data %>%
  mutate(word_count = str_count(reviewText, "\\S+")) %>%
  group_by(overall) %>%
  summarise(
    avg_word_count = mean(word_count),
    total_reviews = n()
  )

# 4. Save Results
message("Analysis complete. Saving results...")
write.csv(analysis, "rating_analysis_results.csv", row.names = FALSE)

print(analysis)
