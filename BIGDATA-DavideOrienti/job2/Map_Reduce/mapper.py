#!/usr/bin/env python3
import sys
import csv
import re

def get_price_range(price):
    price = float(price)
    if price < 20000:
        return "low"
    elif price <= 50000:
        return "medium"
    else:
        return "high"

for line in sys.stdin:
    row = next(csv.reader([line.strip()]))

    if row[0] == "make_name":  # Salta intestazione
        continue

    city = row[4].strip()
    year = row[3].strip()
    price = row[2].strip()
    days = row[5].strip()
    description = row[8].strip()

    if not (city and year and price and days):
        continue

    try:
        price_range = get_price_range(float(price))
        key = f"{city},{year},{price_range}"
        # Rimuove punteggiatura e tokenizza parole
        words = re.findall(r'\b\w+\b', description.lower())
        value = f"1,{days}," + "|".join(words)
        print(f"{key}\t{value}")
    except:
        continue
