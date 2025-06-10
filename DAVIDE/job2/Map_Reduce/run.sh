#!/bin/bash

# Percorsi relativi alla tua struttura
INPUT="../../data/dataset_full_cleaned.csv"
MAPPER="./mapper.py"
REDUCER="./reduce.py"
OUTPUT="../../data/job2_output.txt"

# Esecuzione MapReduce in locale
cat "$INPUT" | python3 "$MAPPER" | sort | python3 "$REDUCER" > "$OUTPUT"

# Mostra prime 10 righe
echo -e "\n--- Prime 10 righe Job 2 ---"
head -n 10 "$OUTPUT"
