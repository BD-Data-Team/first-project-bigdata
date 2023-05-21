#!/usr/bin/env python3
"""mapper.py"""

import sys
import csv
from datetime import datetime

cols = ['Id', 'ProductId', 'UserId', 'ProfileName', 'HelpfulnessNumerator',
        'HelpfulnessDenominator', 'Score', 'Time', 'Summary', 'Text']

# read lines from STDIN (standard input)
for line in sys.stdin:
    row = dict(zip(cols, [a.strip() for a in next(csv.reader([line]))]))

    if row['Id'] == 'Id':
        continue
    try:
        year = datetime.fromtimestamp(
            int(row['Time'])).year  # year of the review
    except:
        continue

    text = row['Text']  # text of the review
    product_id = row['ProductId']  # product id of the item reviewed

    print(f"{year}\t{product_id}\t{text}")
