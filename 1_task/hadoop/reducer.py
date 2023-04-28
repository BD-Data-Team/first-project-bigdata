#!/usr/bin/env python3
"""reducer.py"""

import sys

# this dictionary maps each word to the sum of the values
# that the mapper has computed for that word
count = 0

# input comes from STDIN
# note: this is the output from the mapper!
for line in sys.stdin:

    # as usual, remove leading/trailing spaces
    line = line.strip()

    # parse the input elements
    _, current_count = line.split("\t")

    # convert count (currently a string) to int
    try:
        current_count = int(current_count)
    except ValueError:
        # count was not a number, so
        # silently ignore/discard this line
        continue

    count += current_count

print(count)


#!/usr/bin/env python3
"""reducer.py"""


# this dictionary maps each bigram to the sum of the values
# that the mapper has computed for that bigram
year_for_product_2_sum = {}

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

    year_for_product_2_sum += cur_count

for bigram in bigram_2_sum:
    word_1, word_2 = bigram
    print("%s, %s: %i" % (word_1, word_2, bigram_2_sum[bigram]))
