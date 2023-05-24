#!/usr/bin/env python3
"""mapper.py"""

import sys

NUM_PARTITION = 4

# read lines from STDIN (standard input)
for line in sys.stdin:
    line = line.strip()
    products_str, users_str = line.split("\t")
    
    if users_str == "":
        continue

    users = users_str.split(",")

    if len(users) <= 1:
        continue

    # n in [0, NUM_PARTITION]
    n = hash(line) % NUM_PARTITION
    line = line.replace('\t', '|')
    
    for i in range(NUM_PARTITION):
        print(f"{n}-{i}\tA--{line}")
        print(f"{i}-{n}\tB--{line}")





