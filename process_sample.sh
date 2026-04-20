#!/bin/bash

cat sample_reviews.json \
| grep -o '"reviewText": "[^"]*"' \
| sed 's/"reviewText": "//g' \
| sed 's/"$//g' \
| tr 'A-Z' 'a-z' \
| tr -c 'a-z\n' ' ' \
| tr ' ' '\n' \
| grep -v '^$' \
| sort \
| uniq -c \
| sort -nr \
| head
