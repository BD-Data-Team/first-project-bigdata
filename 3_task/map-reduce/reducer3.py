#!/usr/bin/env python3
"""mapper.py"""

import sys

products_2_users = {}

# read lines from STDIN (standard input)
for line in sys.stdin:
    line = line.strip()
    _, products, users = line.split('\t')

    products_2_users[products] = ",".join(sorted(users.split(',')))

for products, users in products_2_users.items():
    products_str = ",".join(sorted(products.split(',')))
    print(f"{products_str} -> {users}")