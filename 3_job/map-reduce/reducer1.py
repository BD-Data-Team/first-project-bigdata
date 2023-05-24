#!/usr/bin/env python3
"""mapper.py"""

import sys
from collections import Counter

product_2_user = {}
product_2_count = Counter()

# read lines from STDIN (standard input)
for line in sys.stdin:
    line = line.strip()
    
    product_id, user_id = line.split('\t')

    if product_id not in product_2_user:
        product_2_user[product_id] = set()
    
    
    product_2_user[product_id].add(user_id)
    product_2_count[product_id] += 1


for product_id, users in product_2_user.items():
    if product_2_count[product_id] < 3 or len(users) < 2:
        continue

    users_str = ",".join(users)
    print(f"{product_id}\t{users_str}")