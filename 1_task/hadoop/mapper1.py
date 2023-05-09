#!/usr/bin/env python3
"""mapper.py"""

import sys
import csv

cols = ['Id', 'ProductId', 'UserId', 'ProfileName', 'HelpfulnessNumerator',
        'HelpfulnessDenominator', 'Score', 'Time', 'Summary', 'Text']

# read lines from STDIN (standard input)
for line in sys.stdin:
    row = dict(zip(cols, [a.strip() for a in next(csv.reader([line]))]))

    if row['Id'] == 'Id' or row['UserId'] == '':
        continue

    year = row['Time'].split("-")[0]  # year of the review
    text = row['Text']  # text of the review
    product_id = row['ProductId']  # product id of the item reviewed

    print(f"{year}-{product_id}\t{text}\t{1}")
