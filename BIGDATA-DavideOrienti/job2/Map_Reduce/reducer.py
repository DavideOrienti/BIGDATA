#!/usr/bin/env python3
import sys
from collections import Counter, defaultdict

current_key = None
total_count = 0
total_days = 0
word_counter = Counter()

for line in sys.stdin:
    key, value = line.strip().split('\t', 1)
    
    if current_key != key:
        if current_key is not None:
            avg_days = total_days / total_count if total_count else 0
            top_words = [w for w, _ in word_counter.most_common(3)]
            print(f"{current_key}\tCount: {total_count}, Avg Days: {avg_days:.1f}, Top Words: {top_words}")

        # reset
        current_key = key
        total_count = 0
        total_days = 0
        word_counter = Counter()

    parts = value.split(",", 2)
    if len(parts) < 3:
        continue
    try:
        count = int(parts[0])
        days = float(parts[1])
        words = parts[2].split("|") if parts[2] else []

        total_count += count
        total_days += days
        word_counter.update(words)
    except:
        continue

# stampa ultimo gruppo
if current_key is not None:
    avg_days = total_days / total_count if total_count else 0
    top_words = [w for w, _ in word_counter.most_common(3)]
    print(f"{current_key}\tCount: {total_count}, Avg Days: {avg_days:.1f}, Top Words: {top_words}")
