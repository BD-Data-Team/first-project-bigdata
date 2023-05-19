#!/usr/bin/env python3
"""mapper.py"""

import sys
import collections

# read lines from STDIN (standard input)
for line in sys.stdin:
    line = line.strip()

    # split the current line into key and sentence
    year, product_id, text = line.split("\t")

    words = text.split(" ")

    for word, count in collections.Counter(words).items():
        if len(word) >= 4 and word != "":
            print(f"{word}\t{year}\t{product_id}\t{count}")


# TODO: provare il conteggio dividendo l'operazione su mapper e reducer
