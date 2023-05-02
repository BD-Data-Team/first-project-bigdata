#!/usr/bin/env python3
"""reducer.py"""

import sys
import collections

# this dictionary maps each bigram to the sum of the values
# that the mapper has computed for that bigram
year_for_product_2_sum = {}

product_id_2_text = {}

# input comes from STDIN
# note: this is the output from the mapper!
for line in sys.stdin:
    # as usual, remove leading/trailing spaces
    line = line.strip()

    # parse the input elements
    year_for_product, text, count = line.split("\t")

    year = year_for_product.split("-")[0]
    product_id = year_for_product.split("-")[1]

    # convert count (currently a string) to int
    # if count is not a number silently ignore/discard this line
    try:
        cur_count = int(count)
    except ValueError:
        continue


    if product_id not in product_id_2_text:
        product_id_2_text[product_id] = []

    product_id_2_text[product_id].append(text)

    if year not in year_for_product_2_sum:
        year_for_product_2_sum[year] = collections.Counter()
    
    year_for_product_2_sum[year][0][product_id] += cur_count
    year_for_product_2_sum[year][1]


for year in year_for_product_2_sum:
    for product in year_for_product_2_sum[year].most_common(10):
        print(f"{year}\t{product[0]}\t{product[1]}")
