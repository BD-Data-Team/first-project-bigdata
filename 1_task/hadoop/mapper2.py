#!/usr/bin/env python3
"""mapper.py"""

import sys

# read lines from STDIN (standard input)
for line in sys.stdin:

    # removing leading/trailing whitespaces
    line = line.strip()

    # split the current line into key and sentence
    year, product_id, text = line.split("\t")

    words = text.split(" ")

    for word in words:
        if word.count() >= 4:
            print(f"{year}-{product_id}\t{word}\t{1}")
