#!/bin/bash

# Percorsi input/output

INPUT_PATH="../../data/dataset_full_cleaned.csv"
OUTPUT_PATH="../../output/"
OUTPUT_FILE="${OUTPUT_PATH}job1_sparksql_preview.txt"

# Assicurati che la cartella output esista
mkdir -p "$OUTPUT_PATH"

# Esegui lo script PySpark e salva le prime 10 righe in un file .txt
#spark-submit job1_sparksql.py > "$OUTPUT_FILE"
#spark-submit job1_sparksql.py --input "$INPUT_PATH" > "$OUTPUT_FILE"
spark-submit job1_sparksql.py -input "$INPUT_PATH" -output "$OUTPUT_PATH"

# Notifica completamento
echo "Esecuzione completata. Output salvato in: $OUTPUT_FILE"

#echo -e "\nPrime 10 righe:"
#head "$OUTPUT_FILE"
