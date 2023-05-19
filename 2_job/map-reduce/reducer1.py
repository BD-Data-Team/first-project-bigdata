#!/usr/bin/env python3

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

for user in user_2_count:
    appreciation = user_2_usefulness[user] / user_2_count[user]
    print(f"{user}\t{appreciation}")
