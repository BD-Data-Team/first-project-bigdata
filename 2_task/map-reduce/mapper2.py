#!/usr/bin/env python3
"""mapper.py"""

import sys

# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading/trailing spaces
    line = line.strip()

    # parse the input elements
    user, appreciation = line.split("\t")

    print(f"{1 - float(appreciation)}\t{user}")