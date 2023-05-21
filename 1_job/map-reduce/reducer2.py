#!/usr/bin/env python3
"""reducer.py"""

import sys
import collections

year_for_word_2_count = {}

# input comes from STDIN
for line in sys.stdin:
    line = line.strip()

    # parse the input elements
    word, year, product_id, count = line.split("\t")

    # convert count (currently a string) to int
    # if count is not a number silently ignore/discard this line
    try:
        cur_count = int(count)
    except ValueError:
        continue

    if year not in year_for_word_2_count:
        year_for_word_2_count[year] = {}

    if product_id not in year_for_word_2_count[year]:
        year_for_word_2_count[year][product_id] = collections.Counter()

    year_for_word_2_count[year][product_id][word] += cur_count


for year in year_for_word_2_count:
    for product in year_for_word_2_count[year]:
        for word, count in year_for_word_2_count[year][product].most_common(5):
            print(f"{year}\t{product}\t{word}\t{count}")
