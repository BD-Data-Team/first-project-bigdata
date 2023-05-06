#!/usr/bin/env python3
"""reducer.py"""

import sys

for line in sys.stdin:
    line = line.strip()

    appreciation, user = line.split("\t")
    
    print(f"{user}\t{appreciation}")