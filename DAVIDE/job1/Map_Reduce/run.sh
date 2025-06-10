#!/bin/bash

# Percorsi relativi alla tua struttura
INPUT="../../data/dataset_full_cleaned.csv"
MAPPER="./mapper.py"
REDUCER="./reducer.py"
OUTPUT="../../data/job1_output.txt"

# Esecuzione MapReduce in locale
cat "$INPUT" | python3 "$MAPPER" | sort | python3 "$REDUCER" > "$OUTPUT"

# Mostra prime 10 righe
echo -e "\n--- Prime 10 righe Job 1 ---"
head -n 10 "$OUTPUT"
