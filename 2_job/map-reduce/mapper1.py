#!/usr/bin/env python3

import sys
import csv

cols = ['Id', 'ProductId', 'UserId', 'ProfileName', 'HelpfulnessNumerator',
        'HelpfulnessDenominator', 'Score', 'Year', 'Summary', 'Text']

# read lines from STDIN (standard input)
for line in sys.stdin:

    row = dict(zip(cols, [a.strip() for a in next(csv.reader([line]))]))

    # skip CSV header
    if row['Id'] == 'Id':
        continue

    try:
        helpfulness_num = int(row['HelpfulnessNumerator'])
        helpfulness_den = int(row['HelpfulnessDenominator'])
    except ValueError:
        continue

    if helpfulness_num > helpfulness_den or helpfulness_den <= 0:
        continue

    usefulness = helpfulness_num / helpfulness_den

    print(f"{row['UserId']}\t{usefulness}")
