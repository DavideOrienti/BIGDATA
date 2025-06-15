#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, explode, split, lower, regexp_extract, count, avg
from pyspark.sql.types import IntegerType, DoubleType, StringType, StructType, StructField
import argparse
import os

# 1. Argparse per input/output
parser = argparse.ArgumentParser()
parser.add_argument("-input", type=str, help="Path to input file")
parser.add_argument("-output", type=str, help="Path to output folder")
args = parser.parse_args()

# 2. Avvia SparkSession
spark = SparkSession.builder \
    .appName("Job 2 - Spark SQL") \
    .getOrCreate()

# 3. Definisci schema dei dati
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

schema = StructType([
    StructField("make_name", StringType(), True),
    StructField("model_name", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("year", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("daysonmarket", IntegerType(), True),
    StructField("horsepower", DoubleType(), True),
    StructField("engine_displacement", DoubleType(), True),
    StructField("description", StringType(), True)
])


# 4. Carica il dataset
df = spark.read \
    .option("header", True) \
    .schema(schema) \
    .csv(args.input)


# 5. Definisci la fascia di prezzo
df = df.withColumn(
    "price_band",
    when(col("price") > 50000, "high")
    .when((col("price") >= 20000) & (col("price") <= 50000), "medium")
    .otherwise("low")
)

# 6. Prepara la tabella temporanea
df.createOrReplaceTempView("cars")

# 7. Query SQL per conteggio auto e media daysonmarket
query_stats = """
    SELECT
        city,
        year,
        price_band,
        COUNT(*) AS num_cars,
        ROUND(AVG(daysonmarket), 2) AS avg_daysonmarket
    FROM cars
    GROUP BY city, year, price_band
"""
stats_df = spark.sql(query_stats)

# 8. Preparazione parole chiave (tokenize descrizione)
words_df = df.select(
    col("city"),
    col("year"),
    col("price_band"),
    explode(split(lower(col("description")), "\\s+")).alias("word")
).filter(col("word").rlike("^[a-zA-Z]+$"))  # filtra solo parole alfabetiche

# 9. Conta le parole più frequenti per (city, year, price_band)
words_df.createOrReplaceTempView("words")

query_words = """
    SELECT
        city,
        year,
        price_band,
        word,
        COUNT(*) AS word_count
    FROM words
    GROUP BY city, year, price_band, word
    ORDER BY city, year, price_band, word_count DESC
"""
word_counts_df = spark.sql(query_words)

# 10. Seleziona le prime 3 parole più frequenti per ogni gruppo (city, year, price_band)
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, collect_list

windowSpec = Window.partitionBy("city", "year", "price_band").orderBy(col("word_count").desc())
top_words_df = word_counts_df.withColumn("rank", row_number().over(windowSpec)) \
                             .filter(col("rank") <= 3) \
                             .groupBy("city", "year", "price_band") \
                             .agg(collect_list("word").alias("top_3_words"))

# 11. Join statistiche e parole
final_df = stats_df.join(
    top_words_df,
    on=["city", "year", "price_band"],
    how="left"
)

final_df.show(10, truncate=False)

# Recupera il path di output dall'argomento
output_file_path = os.path.join(args.output, "job2_sparksql_preview.txt")

# Salva la preview nel file in formato tabellare
with open(output_file_path, "w") as f:
    f.write(final_df._jdf.showString(10, 0, False))

# 14. Stop Spark
spark.stop()
