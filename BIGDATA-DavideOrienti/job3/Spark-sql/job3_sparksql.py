#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round as spark_round, max as spark_max
from pyspark.sql.types import IntegerType, DoubleType, StringType, StructType, StructField
import argparse

# 1. Parsing argomenti
parser = argparse.ArgumentParser()
parser.add_argument("-input", type=str, help="Path to input file")
parser.add_argument("-output", type=str, help="Path to output folder")
args = parser.parse_args()

# 2. Inizializza SparkSession
spark = SparkSession.builder \
    .appName("spark-sql#job-3") \
    .getOrCreate()

# 3. Definizione schema
schema = StructType([
    StructField("make_name", StringType(), True),
    StructField("model_name", StringType(), True),
    StructField("horsepower", DoubleType(), True),
    StructField("engine_displacement", DoubleType(), True),
    StructField("price", DoubleType(), True)
])

# 4. Lettura dataset
df = spark.read.csv(args.input, header=True, schema=schema)
df = df.filter(
    (col("horsepower").isNotNull()) &
    (col("engine_displacement").isNotNull()) &
    (col("price").isNotNull())
)

# 5. Crea una vista temporanea
df.createOrReplaceTempView("used_cars")

# 6. Query Spark SQL
query = """
SELECT
    a.model_name as model_a,
    b.model_name as model_b,
    a.horsepower as horsepower_a,
    b.horsepower as horsepower_b,
    a.engine_displacement as displacement_a,
    b.engine_displacement as displacement_b,
    b.price as price_b
FROM
    used_cars a
JOIN
    used_cars b
ON
    ABS(a.horsepower - b.horsepower) <= 0.1 * a.horsepower AND
    ABS(a.engine_displacement - b.engine_displacement) <= 0.1 * a.engine_displacement
"""

similar_cars = spark.sql(query)
similar_cars.createOrReplaceTempView("similar_groups")

# 7. Calcola prezzo medio e modello con potenza massima per ogni gruppo
result = spark.sql("""
SELECT
    model_a as reference_model,
    COUNT(DISTINCT model_b) as num_similar_models,
    ROUND(AVG(price_b), 2) as avg_price_similar_models,
    FIRST(model_b) as top_model
FROM (
    SELECT
        model_a,
        model_b,
        price_b,
        ROW_NUMBER() OVER (PARTITION BY model_a ORDER BY horsepower_b DESC) as rn
    FROM similar_groups
) ranked
WHERE rn = 1
GROUP BY model_a
ORDER BY reference_model
""")

result.show(10, truncate=False)

# 8. Output CSV (opzionale)
# result.coalesce(1).write.mode("overwrite").option("header", True).csv(args.output)

spark.stop()
