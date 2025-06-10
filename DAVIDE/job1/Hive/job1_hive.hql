-- Crea il database se non esiste
CREATE DATABASE IF NOT EXISTS car_data;
USE car_data;

-- Elimina la tabella se esiste già
DROP TABLE IF EXISTS used_cars;

-- Crea la tabella per leggere il CSV pulito
CREATE EXTERNAL TABLE used_cars (
    make_name STRING,
    model_name STRING,
    price FLOAT,
    year INT,
    city STRING,
    daysonmarket INT,
    horsepower FLOAT,
    engine_displacement FLOAT,
    description STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/car_data/input'
TBLPROPERTIES ("skip.header.line.count"="1");

-- Crea tabella di output con le statistiche per marca e modello
DROP TABLE IF EXISTS job1_stats;

CREATE TABLE job1_stats AS
SELECT
    make_name,
    model_name,
    COUNT(*) AS total_cars,
    MIN(price) AS min_price,
    MAX(price) AS max_price,
    ROUND(AVG(price), 2) AS avg_price,
    SORT_ARRAY(COLLECT_SET(year)) AS year_list
FROM
    used_cars
GROUP BY
    make_name, model_name;

-- Mostra i primi 10 risultati
SELECT * FROM job1_stats LIMIT 10;
