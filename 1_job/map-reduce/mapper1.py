#!/usr/bin/env python3
"""mapper.py"""

import sys
import csv

cols = ['Id', 'ProductId', 'UserId', 'ProfileName', 'HelpfulnessNumerator',
        'HelpfulnessDenominator', 'Score', 'Year', 'Summary', 'Text']

# read lines from STDIN (standard input)
for line in sys.stdin:
    
    line = line.strip()
    row = dict(zip(cols, [a for a in next(csv.reader([line]))]))

    if row['Id'] == 'Id':
        continue
    try:
        year = int(row['Year'])  # year of the review
    except:
        continue

    product_id = row['ProductId']  # product id of the item reviewed
    text = row['Text']  # text of the review

    print(f"{year}\t{product_id}\t{text}")
