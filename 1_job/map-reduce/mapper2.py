#!/usr/bin/env python3
"""mapper.py"""

import sys
import collections

# read lines from STDIN (standard input)
for line in sys.stdin:
    line = line.strip()

    # split the current line into key and sentence
    year, product_id, texts = line.split("\t")

    words = texts.split(" ")

    for word, count in collections.Counter(words).items():
        if len(word) >= 4:
            print(f"{word}\t{year}\t{product_id}\t{count}")

