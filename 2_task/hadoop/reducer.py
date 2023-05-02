#!/usr/bin/env python3
"""reducer.py"""

import sys
import collections

user_2_count = collections.Counter()
user_2_usefulness = collections.Counter()

# input comes from STDIN
# note: this is the output from the mapper!
for line in sys.stdin:
    # as usual, remove leading/trailing spaces
    line = line.strip()

    # parse the input elements
    id, usefulness = line.split("\t")

    # convert count (currently a string) to int
    # if count is not a number silently ignore/discard this line
    try:
        curr_usefulness = float(usefulness)
    except ValueError:
        continue

    user_2_count[id] += 1
    user_2_usefulness[id] += curr_usefulness

user_2_appreciation = {}
for user in user_2_count:
    user_2_appreciation[user] = user_2_usefulness[user] / user_2_count[user]

# sort the user_2_appreciation by value
user_2_appreciation_ordered = dict(sorted(user_2_appreciation.items(), key=lambda item: item[1], reverse=True))

for user in user_2_appreciation_ordered:
    print(f"{user}\t{user_2_appreciation_ordered[user]}")


