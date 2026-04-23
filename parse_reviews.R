
if (!file.exists("stopwords.txt")) {
  stop("stopwords.txt not found! Make sure to upload it.")
}
stop_words <- readLines("stopwords.txt")

unzip("reviews.zip")
files <- list.files(pattern = "\\.csv$") 
df <- read.csv(files[1], stringsAsFactors = FALSE)

high_df <- df[df$overall >= 4, ]
low_df  <- df[df$overall <= 2, ]

get_top_20 <- function(text_vector, stops) {
  words <- unlist(strsplit(tolower(text_vector), "\\W+"))
  words <- words[words != "" & !(words %in% stops)]
  
  word_freq <- as.data.frame(table(words), stringsAsFactors = FALSE)
  colnames(word_freq) <- c("word", "count")
  word_freq <- word_freq[order(-word_freq$count), ]
  
  return(head(word_freq, 20))
}

high_top_20 <- get_top_20(high_df$reviewText, stop_words)
low_top_20  <- get_top_20(low_df$reviewText, stop_words)

write.csv(high_top_20, "high_top_20.csv", row.names = FALSE)
write.csv(low_top_20, "low_top_20.csv", row.names = FALSE)

print("Processing complete. Top 20 files created.")
