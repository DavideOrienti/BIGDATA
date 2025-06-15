#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, min, max, avg, collect_set
import argparse
import os

# 1. Parsing argomenti
parser = argparse.ArgumentParser()
parser.add_argument("-input", type=str, required=True, help="Path to input CSV file")
parser.add_argument("-output", type=str, required=True, help="Path to output folder")
args = parser.parse_args()

# 2. Avvia Spark
spark = SparkSession.builder \
    .appName("Job1 - Statistiche Marca e Modello Auto") \
    .getOrCreate()

# 3. Carica il dataset
df = spark.read.option("header", True).option("inferSchema", True).csv(args.input)

# 4. Pulizia dati base
df = df.filter(df.price.isNotNull())
df = df.filter(df.make_name.isNotNull() & df.model_name.isNotNull())

# 5. Crea vista SQL temporanea
df.createOrReplaceTempView("used_cars")

# 6. Query SQL
query = """
    SELECT
        make_name,
        model_name,
        COUNT(*) AS num_cars,
        MIN(price) AS min_price,
        MAX(price) AS max_price,
        ROUND(AVG(price), 2) AS avg_price,
        COLLECT_SET(year) AS years_present
    FROM used_cars
    GROUP BY make_name, model_name
    ORDER BY make_name, model_name
"""

result_df = spark.sql(query)

# 7. Visualizza output
result_df.show(10, truncate=False)

# 8bis. Salva anche le prime 10 righe in un file leggibile
output_preview_file = os.path.join(args.output, "job1_sparksql_preview.txt")

with open(output_preview_file, "w") as f:
    f.write(result_df._jdf.showString(10, 0, False))


# 8. Salva l'output
result_df.coalesce(1).write.mode("overwrite").option("header", True).csv(args.output)

# 9. Ferma Spark
spark.stop()