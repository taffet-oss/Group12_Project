#!/bin/bash

cat high_reviews.json \
| grep -o '"reviewText": "[^"]*"' \
| sed 's/"reviewText": "//g' \
| sed 's/"$//g' \
| tr 'A-Z' 'a-z' \
| tr -c 'a-z\n' ' ' \
| tr ' ' '\n' \
| grep -v '^$' \
| grep -v -E '^(the|to|i|it|and|a|this|is|my|of|t|s|for|you|that|with|but|have|in|on|be|can|me|she|so)$' \
| sort \
| uniq -c \
| sort -nr \
| head -20
