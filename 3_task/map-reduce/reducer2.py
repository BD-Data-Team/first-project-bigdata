#!/usr/bin/env python3
"""mapper.py"""

import sys

# farò qui il prodotto cartesiano
# https://stackoverflow.com/questions/11066434/how-to-implement-self-join-cross-product-with-hadoop

remaining_lines = sys.stdin.readlines()
for line1 in remaining_lines:
    remaining_lines.remove(line1)
    
    line1 = line1.strip()
    products1, users1 = line1.split('\t')
    
    products1 = set(products1.split(","))
    users1 = set(users1.split(","))
    
    for line2 in remaining_lines:
        line2 = line2.strip()
        products2, users2 = line2.split('\t')


        products2 = set(products2.split(","))
        users2 = set(users2.split(","))

        products = products1.union(products2)
        common_users = users1.intersection(users2)

        if common_users == set():
            continue

        common_users_str = ",".join(common_users)
        products_str = ",".join(products)

        print(f"{products_str}\t{common_users_str}")
