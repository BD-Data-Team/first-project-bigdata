#!/usr/bin/env python3
"""mapper.py"""

import sys


# read lines from STDIN (standard input)
for line in sys.stdin:
    line = line.strip()
    
    products_str, users_str = line.split("\t")
    
    if users_str == "":
        continue

    users = users_str.split(",")

    if len(users) < 3:
        continue

    print(line)




