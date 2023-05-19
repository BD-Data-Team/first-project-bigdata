#!/usr/bin/env python3
"""mapper.py"""

# TODO: capire come mei per questo utente "A161DK06JJMCYF" ci viene una usefulness di 1.14

import sys
import csv

cols = ['Id', 'ProductId', 'UserId', 'ProfileName', 'HelpfulnessNumerator',
        'HelpfulnessDenominator', 'Score', 'Time', 'Summary', 'Text']

# read lines from STDIN (standard input)
for line in sys.stdin:

    row = dict(zip(cols, [a.strip() for a in next(csv.reader([line]))]))
    
    if row['Id'] == 'Id':
        continue

    id = row['UserId'] 

    try:
        helpfulness_numerator = int(row['HelpfulnessNumerator'])
        helpfulness_denominator = int(row['HelpfulnessDenominator'])
    except ValueError:
        continue

    if helpfulness_numerator > helpfulness_denominator or helpfulness_denominator <= 0:
        continue
    
    usefulness = helpfulness_numerator / helpfulness_denominator

    print(f"{id}\t{usefulness}")
