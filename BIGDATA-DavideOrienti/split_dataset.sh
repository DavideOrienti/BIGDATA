#!/bin/bash

# Percorso al dataset completo
INPUT="./data/dataset_full_cleaned.csv"

# Cartella di destinazione
OUTPUT_DIR="./data"

# Assicurati che esista
mkdir -p "$OUTPUT_DIR"

echo "🔧 Inizio generazione subset da $INPUT..."

# Header
HEADER=$(head -n 1 "$INPUT")

# 100k
echo "$HEADER" > "$OUTPUT_DIR/100k.csv"
head -n 100001 "$INPUT" | tail -n +2 >> "$OUTPUT_DIR/100k.csv"
echo "✅ Creato 100k.csv"

# 1M
echo "$HEADER" > "$OUTPUT_DIR/1M.csv"
head -n 1000001 "$INPUT" | tail -n +2 >> "$OUTPUT_DIR/1M.csv"
echo "✅ Creato 1M.csv"

# 3M (copia completa)
cp "$INPUT" "$OUTPUT_DIR/3M.csv"
echo "✅ Copiato 3M.csv"

echo "🎉 Tutti i file generati in: $OUTPUT_DIR"