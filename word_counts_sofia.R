library(jsonlite)

args <- commandArgs(trailingOnly = TRUE)
input_path <- if (length(args) >= 1) args[1] else "All_Amazon_Review.json"
top_n <- if (length(args) >= 2) as.integer(args[2]) else 20L

message("Reading dataset from: ", input_path)
message("Computing top ", top_n, " words for low and high reviews")

stop_words <- c(
  "the", "to", "i", "it", "and", "a", "this", "is", "my", "of", "t", "s",
  "for", "you", "that", "with", "but", "have", "in", "on", "be", "can",
  "me", "she", "so", "was", "as", "they", "are", "not", "we", "he", "her",
  "him", "his", "at", "if", "or", "from", "all", "an", "has", "had", "them",
  "were", "your", "when", "will", "would", "could", "get", "got", "just",
  "really", "also", "very", "its", "im", "ive", "youre", "dont", "doesnt",
  "didnt"
)

normalize_tokens <- function(text) {
  text <- tolower(text)
  text <- gsub("[\r\n\t]", " ", text)
  text <- gsub("['`]", "", text)
  text <- gsub("[^a-z]+", " ", text)
  tokens <- unlist(strsplit(text, "\\s+"), use.names = FALSE)
  tokens <- tokens[tokens != ""]
  tokens[tokens == "don"] <- "dont"
  tokens[tokens == "didn"] <- "didnt"
  tokens[tokens == "doesn"] <- "doesnt"
  tokens[!(tokens %in% stop_words)]
}

increment_counts <- function(tokens, env) {
  if (length(tokens) == 0) {
    return(invisible(NULL))
  }

  token_table <- table(tokens)
  token_names <- names(token_table)

  for (i in seq_along(token_names)) {
    token <- token_names[[i]]
    current <- if (exists(token, envir = env, inherits = FALSE)) {
      get(token, envir = env, inherits = FALSE)
    } else {
      0L
    }
    assign(token, current + as.integer(token_table[[i]]), envir = env)
  }

  invisible(NULL)
}

env_to_counts <- function(env, group_name) {
  words <- ls(envir = env, all.names = TRUE)
  if (length(words) == 0) {
    return(data.frame(group = character(), word = character(), count = integer()))
  }

  counts <- unlist(mget(words, envir = env, inherits = FALSE), use.names = FALSE)
  data.frame(
    group = group_name,
    word = words,
    count = as.integer(counts),
    stringsAsFactors = FALSE
  )
}

low_counts <- new.env(hash = TRUE, parent = emptyenv())
high_counts <- new.env(hash = TRUE, parent = emptyenv())

malformed_lines <- 0L
complete_records <- 0L
low_reviews <- 0L
high_reviews <- 0L

con <- file(input_path, open = "r")
on.exit(close(con), add = TRUE)

repeat {
  line <- readLines(con, n = 1, warn = FALSE)
  if (length(line) == 0) {
    break
  }

  line <- trimws(line)
  if (line == "") {
    next
  }

  record <- tryCatch(fromJSON(line), error = function(e) NULL)
  if (is.null(record)) {
    malformed_lines <- malformed_lines + 1L
    next
  }

  complete_records <- complete_records + 1L

  rating <- record[["overall"]]
  review_text <- record[["reviewText"]]

  if (is.null(rating) || is.null(review_text) || is.na(review_text) || review_text == "") {
    next
  }

  rating <- as.integer(rating)
  tokens <- normalize_tokens(review_text)

  if (rating %in% c(1L, 2L)) {
    low_reviews <- low_reviews + 1L
    increment_counts(tokens, low_counts)
  } else if (rating %in% c(4L, 5L)) {
    high_reviews <- high_reviews + 1L
    increment_counts(tokens, high_counts)
  }
}

all_counts <- rbind(
  env_to_counts(low_counts, "low"),
  env_to_counts(high_counts, "high")
)

all_counts <- all_counts[order(all_counts$group, -all_counts$count, all_counts$word), ]

top_low <- subset(all_counts, group == "low")
top_low <- head(top_low[order(-top_low$count, top_low$word), c("word", "count")], top_n)

top_high <- subset(all_counts, group == "high")
top_high <- head(top_high[order(-top_high$count, top_high$word), c("word", "count")], top_n)

write.csv(all_counts, "word_counts_by_group.csv", row.names = FALSE)
write.csv(top_low, "top_low_words.csv", row.names = FALSE)
write.csv(top_high, "top_high_words.csv", row.names = FALSE)

message("Analysis complete. Saving results...")
message("Complete records processed: ", complete_records)
message("Low reviews counted: ", low_reviews)
message("High reviews counted: ", high_reviews)
message("Skipped malformed lines: ", malformed_lines)

print(top_low)
print(top_high)
