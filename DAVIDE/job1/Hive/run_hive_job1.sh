#!/bin/bash

# Esegue lo script Hive per JOB 1
echo "==> Avvio JOB 1 Hive..."
hive -f job1/Map_Reduce/job1_hive.hql

# Visualizza i primi 10 risultati
echo -e "\n==> Prime 10 righe del risultato di JOB 1:"
hive -e "USE car_data; SELECT * FROM job1_stats LIMIT 10;"
