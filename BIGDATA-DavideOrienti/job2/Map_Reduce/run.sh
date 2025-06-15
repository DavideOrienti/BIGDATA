#!/bin/bash

# Percorsi relativi alla tua struttura
INPUT="../../data/dataset_full_cleaned.csv"
MAPPER="./mapper.py"
REDUCER="./reducer.py"
OUTPUT="../../output/job2_MapReduce_output.txt"
OUTPUT_FIRST10="../../output/job2_MapReduce_output_first10.txt"

# Esecuzione MapReduce in locale
cat "$INPUT" | python3 "$MAPPER" | sort | python3 "$REDUCER" > "$OUTPUT"

# Mostra prime 10 righe
echo -e "\n--- Prime 10 righe Job 2 ---"
head -n 10 "$OUTPUT"

# Salva le prime 10 righe in un file separato
head -n 10 "$OUTPUT" > "$OUTPUT_FIRST10"

echo -e "\nPrime 10 righe salvate in: $OUTPUT_FIRST10"
