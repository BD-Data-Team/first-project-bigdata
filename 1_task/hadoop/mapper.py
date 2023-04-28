#!/usr/bin/env python3
"""mapper.py"""

import sys

# read lines from STDIN (standard input)
for line in sys.stdin:
    if line.split(",")[0] == 'Id':
        continue
    # removing leading/trailing whitespaces
    line = line.strip()

    # split the current line into words
    fields = line.split(",")

    year = fields[7].split('-')[0]  # year of the review
    text = fields[9]  # text of the review
    product_id = fields[1]  # product id of the item reviewed

    print(f"{year}-{product_id}\t{text}\t{1}")
