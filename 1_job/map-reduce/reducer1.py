#!/usr/bin/env python3
"""reducer.py"""

import sys
import collections

year_for_product_2_sum = {}
year_for_product_2_text = {}

# input comes from STDIN
# note: this is the output from the mapper!
for line in sys.stdin:
    # as usual, remove leading/trailing spaces
    line = line.strip()

    # parse the input elements
    year, product_id, text = line.split("\t")


    if year not in year_for_product_2_sum:
        year_for_product_2_sum[year] = collections.Counter()

    if year not in year_for_product_2_text:
        year_for_product_2_text[year] = {}

    if product_id not in year_for_product_2_text[year]:
        year_for_product_2_text[year][product_id] = []

    year_for_product_2_sum[year][product_id] += 1
    year_for_product_2_text[year][product_id].append(text)


for year in year_for_product_2_sum:
    for product in dict(year_for_product_2_sum[year].most_common(10)).keys():
        for text in year_for_product_2_text[year][product]:
            print(year, product, text, sep='\t')
