#!/usr/bin/env python3
import sys

# Salta header
first_line = True

for line in sys.stdin:
    if first_line:
        first_line = False
        continue

    fields = line.strip().split(',')
    if len(fields) < 4:
        continue

    make = fields[0]
    model = fields[1]
    try:
        price = float(fields[2])
        year = fields[3]
    except:
        continue

    key = f"{make}|{model}"
    value = f"{price}|{year}"
    print(f"{key}\t{value}")