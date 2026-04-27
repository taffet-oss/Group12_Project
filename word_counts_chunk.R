library(jsonlite)

args <- commandArgs(trailingOnly = TRUE)
input_path <- if (length(args) >= 1) args[1] else "reviews_chunk_0000"
chunk_id <- if (length(args) >= 2) args[2] else "0"

message("Reading chunk from: ", input_path)
message("Chunk id: ", chunk_id)

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

output_csv <- sprintf("partial_counts_%s.csv", chunk_id)
summary_txt <- sprintf("chunk_summary_%s.txt", chunk_id)

write.csv(all_counts, output_csv, row.names = FALSE, quote = FALSE)

summary_lines <- c(
  paste("chunk_id", chunk_id, sep = ","),
  paste("complete_records", complete_records, sep = ","),
  paste("low_reviews", low_reviews, sep = ","),
  paste("high_reviews", high_reviews, sep = ","),
  paste("malformed_lines", malformed_lines, sep = ",")
)
writeLines(summary_lines, summary_txt)

message("Chunk analysis complete.")
message("Complete records processed: ", complete_records)
message("Low reviews counted: ", low_reviews)
message("High reviews counted: ", high_reviews)
message("Skipped malformed lines: ", malformed_lines)
message("Wrote: ", output_csv)
