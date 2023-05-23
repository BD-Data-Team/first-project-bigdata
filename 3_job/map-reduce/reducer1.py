#!/usr/bin/env python3
"""mapper.py"""

import sys

product_2_user = {}
# user_2_product = {}

# read lines from STDIN (standard input)
for line in sys.stdin:
    line = line.strip()
    
    user_id, product_id = line.split('\t')

    if product_id not in product_2_user:
        product_2_user[product_id] = set()
    
    # if user_id not in user_2_product:
    #     user_2_product[user_id] = set()
    
    product_2_user[product_id].add(user_id)
    # user_2_product[user_id].add(product_id)

for product_id, users in product_2_user.items():
    if len(users) < 3:
        continue
    users_str = ",".join(users)
    print(f"{product_id}\t{users_str}")