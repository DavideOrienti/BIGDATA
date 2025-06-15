#!/bin/bash

# Esegue lo script Hive per JOB 1
echo "==> Avvio JOB 1 Hive..."
#hive -f job1/Hive/job1_hive.hql
hive -f job1_hive.hql

# Percorso dove salvare le prime 10 righe
OUTPUT_FIRST10="../../output/job1_hive_output_first10.txt"

# Visualizza i primi 10 risultati
echo -e "\n==> Prime 10 righe del risultato di JOB 1:"
hive -e "USE car_data; SELECT * FROM job1_stats LIMIT 10;" | tee "$OUTPUT_FIRST10"

echo -e "\nPrime 10 righe salvate in: $OUTPUT_FIRST10"

