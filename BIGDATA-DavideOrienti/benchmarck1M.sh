#!/bin/bash

echo "▶ Esecuzione di tutti gli script per JOB 1 e JOB 2 con dataset 100k"
RESULT_FILE="./output/tempo_totale_1M.txt"
LOG_DIR="./output/logs"
mkdir -p ./output "$LOG_DIR"
echo "Script,Tempo(s)" > "$RESULT_FILE"

# === Imposta dataset da utilizzare ===
DATASET="1M"  # Cambia in "1M" o "3M" per test su altri volumi

echo "📂 Seleziono il dataset: $DATASET"

# Sovrascrivi il file di riferimento per tutti i job
cp "./data/${DATASET}.csv" "./data/dataset_full_cleaned.csv"
hdfs dfs -put -f "./data/${DATASET}.csv" "/user/hive/warehouse/car_data/input/dataset_full_cleaned.csv"

esegui_con_timer() {
  local script="$1"
  local label="$2"
  local log_path="$LOG_DIR/$(basename $script .sh)_100k.log"

  echo -e "\n➡️  Avvio: $label"
  start=$(date +%s)

  # 🔁 Spostati nella directory dello script per mantenere relativi funzionanti
  script_dir=$(dirname "$script")
  (
    cd "$script_dir" || exit 1
    bash "$(basename "$script")"
  ) 2>&1 | tee "$log_path"

  end=$(date +%s)
  tempo=$((end - start))

  echo "$label,$tempo" >> "$RESULT_FILE"
  echo "✅ Completato: $label - Tempo: ${tempo}s"
}

# JOB 1
esegui_con_timer "./job1/Map_Reduce/run.sh" "Job1 MapReduce"
esegui_con_timer "./job1/Hive/run_hive_job1.sh" "Job1 Hive"
esegui_con_timer "./job1/Spark_sql/run.sh" "Job1 SparkSQL"

# JOB 2
esegui_con_timer "./job2/Map_Reduce/run.sh" "Job2 MapReduce"
esegui_con_timer "./job2/Hive/run_hive_job2.sh" "Job2 Hive"
esegui_con_timer "./job2/Spark_sql/run.sh" "Job2 SparkSQL"

echo -e "\n🎉 Tutti gli script sono stati eseguiti. Risultati salvati in: $RESULT_FILE"