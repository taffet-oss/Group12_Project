library(jsonlite)
library(dplyr)
library(stringr)

input_path <- "/staging/taffet/Amazon_Reviews.json" 

message("Reading dataset from: ", input_path)

data <- stream_in(file(input_path))

cleaned_data <- data %>%
  select(overall, reviewText) %>%
  filter(!is.na(reviewText))

analysis <- cleaned_data %>%
  mutate(word_count = str_count(reviewText, "\\S+")) %>%
  group_by(overall) %>%
  summarise(
    avg_word_count = mean(word_count),
    total_reviews = n()
  )

message("Analysis complete. Saving results...")
write.csv(analysis, "rating_analysis_results.csv", row.names = FALSE)

print(analysis)
