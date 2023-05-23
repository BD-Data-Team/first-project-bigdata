#!/usr/bin/env python3
"""mapper.py"""

import sys
import itertools

# farò qui il prodotto cartesiano
# https://stackoverflow.com/questions/11066434/how-to-implement-self-join-cross-product-with-hadoop

a_list, b_list = [], []

for in_line in sys.stdin:
    in_line = in_line.strip()

    _, x_line = in_line.split("\t")
    x, line = x_line.split("--")

    if x == "A":
        a_list.append(line)
    else:
        b_list.append(line)

for line1, line2 in itertools.product(a_list, b_list):
    line1 = line1.strip()
    line2 = line2.strip()

    if line1 == line2:
        continue

    products1, users1 = line1.split('|')
    products2, users2 = line2.split('|')

    products1 = set(products1.split(","))
    users1 = set(users1.split(","))

    products2 = set(products2.split(","))
    users2 = set(users2.split(","))

    products = products1.union(products2)
    common_users = users1.intersection(users2)

    if len(common_users) <= 1:
        continue

    common_users_str = ",".join(common_users)
    products_str = ",".join(products)

    print(f"{products_str}\t{common_users_str}")