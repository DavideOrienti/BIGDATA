#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, min, max, avg, collect_set

# 1. Inizializza la sessione Spark
spark = SparkSession.builder \
    .appName("Job1 - Statistiche Marca e Modello Auto") \
    .getOrCreate()

# 2. Leggi il dataset CSV
input_path = "hdfs:///user/hive/warehouse/car_data/input/dataset_full_cleaned.csv"
 # Sostituisci col path corretto
df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)

# 3. Pulizia dati base (opzionale ma consigliata)
df = df.filter(df.price.isNotNull())
df = df.filter(df.make_name.isNotNull() & df.model_name.isNotNull())

# 4. Crea una vista temporanea per Spark SQL
df.createOrReplaceTempView("used_cars")

# 5. Query SQL
query = """
    SELECT
        make_name,
        model_name,
        COUNT(*) as num_cars,
        MIN(price) as min_price,
        MAX(price) as max_price,
        ROUND(AVG(price), 2) as avg_price,
        COLLECT_SET(year) as years_present
    FROM
        used_cars
    GROUP BY
        make_name, model_name
    ORDER BY
        make_name, model_name
"""

result_df = spark.sql(query)

# 6. Mostra le prime 10 righe
result_df.show(10, truncate=False)

# 7. Salva l'output (opzionale)
output_path = "output/job1_sparksql"
result_df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)

# 8. Chiudi la sessione Spark
spark.stop()
