#!/bin/bash

echo "==> Avvio JOB 2 Hive..."
export HADOOP_CLIENT_OPTS="-Xmx2G"
hive -f job2_hive.hql

OUTPUT_FIRST10="../../output/job2_hive_output_first10.txt"

if hive -e "USE car_data; SHOW TABLES LIKE 'job2_stats';" | grep -q "job2_stats"; then
    echo -e "\n==> Prime 10 righe del risultato di JOB 2:"
    hive -e "USE car_data; SELECT * FROM job2_stats LIMIT 10;" | tee "$OUTPUT_FIRST10"
    echo -e "\nPrime 10 righe salvate in: $OUTPUT_FIRST10"
else
    echo "Tabella job2_stats non trovata, impossibile stampare i risultati."
fi
