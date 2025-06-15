#!/usr/bin/env python3
import sys
from collections import defaultdict

current_key = None
prices = []
years = set()

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    key, value = line.split('\t')
    try:
        price_str, year_str = value.split('|')
        price = float(price_str)
        year = int(year_str)
    except:
        continue

    if current_key == key:
        prices.append(price)
        years.add(year)
    else:
        if current_key and prices:
            make, model = current_key.split('|')
            print(f"{make},{model},{len(prices)},{min(prices):.2f},{max(prices):.2f},{sum(prices)/len(prices):.2f},{sorted(years)}")
        current_key = key
        prices = [price]
        years = {year}

# Output finale
if current_key and prices:
    make, model = current_key.split('|')
    print(f"{make},{model},{len(prices)},{min(prices):.2f},{max(prices):.2f},{sum(prices)/len(prices):.2f},{sorted(years)}")
