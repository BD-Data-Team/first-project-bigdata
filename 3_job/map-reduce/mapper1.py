#!/usr/bin/env python3
"""mapper.py"""

# TODO: provare e capire perchè funziona
# https://groups.google.com/g/mrjob/c/svMJPG9bsfM

import sys
import csv

cols = ['Id', 'ProductId', 'UserId', 'ProfileName', 'HelpfulnessNumerator',
        'HelpfulnessDenominator', 'Score', 'Year', 'Summary', 'Text']

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

    if score >= 4:
        print(f"{product_id}\t{user_id}")
