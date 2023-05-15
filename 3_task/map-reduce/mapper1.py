#!/usr/bin/env python3
"""mapper.py"""

import sys
import csv

cols = ['Id', 'ProductId', 'UserId', 'ProfileName', 'HelpfulnessNumerator',
        'HelpfulnessDenominator', 'Score', 'Time', 'Summary', 'Text']

# read lines from STDIN (standard input)
for line in sys.stdin:
    line = line.strip()
    row = dict(zip(cols, [a.strip() for a in next(csv.reader([line]))]))

    if row['Id'] == 'Id':
        continue

    product_id = row['ProductId']
    user_id = row['UserId']

    try:
        score = int(row['Score'])
    except ValueError:
        continue

    if score >= 4 and product_id != "" and user_id != "":
        print(f"{user_id}\t{product_id}")
