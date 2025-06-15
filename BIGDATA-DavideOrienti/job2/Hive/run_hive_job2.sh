#!/bin/bash

# Esegue lo script Hive per JOB 2
echo "==> Avvio JOB 2 Hive..."
hive -f job2_hive.hql

# Percorso dove salvare le prime 10 righe
OUTPUT_FIRST10="../../output/job2_hive_output_first10.txt"

# Visualizza i primi 10 risultati
echo -e "\n==> Prime 10 righe del risultato di JOB 2:"
hive -e "USE car_data; SELECT * FROM job2_stats LIMIT 10;" | tee "$OUTPUT_FIRST10"

echo -e "\nPrime 10 righe salvate in: $OUTPUT_FIRST10"
