#!/usr/bin/env python3
"""mapper.py"""

import sys

# read lines from STDIN (standard input)
for line in sys.stdin:
    line = line.strip()
    products_str, users_str = line.split('\t')

    users = users_str.split(',')
    
    # filter out list with only one user
    if len(users) < 3:
        continue

    user_0 = users[0]
    print(f"{user_0}\t{products_str}\t{users_str}")

