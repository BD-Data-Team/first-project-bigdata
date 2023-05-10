#!/usr/bin/env python3
"""mapper.py"""

import sys
import csv

cols = ['Id', 'ProductId', 'UserId', 'ProfileName', 'HelpfulnessNumerator',
        'HelpfulnessDenominator', 'Score', 'Time', 'Summary', 'Text']

# read lines from STDIN (standard input)
for line in sys.stdin:

    row = dict(zip(cols, [a.strip() for a in next(csv.reader([line]))]))

    if row['Id'] == 'Id':
        continue

    productId = row['ProductId']
    userId = row['UserId']

    try:
        score = float(row['Score'])
    except ValueError:
        continue

    if score >= 4.0:
        print(f"{userId}\t{productId}")
