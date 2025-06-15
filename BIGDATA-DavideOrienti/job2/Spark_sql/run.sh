#!/bin/bash

#import os

INPUT_PATH="../../data/dataset_full_cleaned.csv"
OUTPUT_PATH="../../output/"
OUTPUT_FILE="${OUTPUT_PATH}job2_sparksql_preview.txt"

mkdir -p "$OUTPUT_PATH"

spark-submit job2_sparksql.py -input "$INPUT_PATH" -output "$OUTPUT_PATH"

if [ $? -eq 0 ]; then
    echo "✅ Esecuzione completata. Output salvato in: $OUTPUT_PATH"
else
    echo "❌ Errore nell'esecuzione di Spark."
fi
