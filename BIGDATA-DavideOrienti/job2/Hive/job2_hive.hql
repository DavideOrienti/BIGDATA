-- Crea il database se non esiste e selezionalo
CREATE DATABASE IF NOT EXISTS car_data;
USE car_data;

-- Crea la tabella di input se non esiste
CREATE EXTERNAL TABLE IF NOT EXISTS used_cars (
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

-- Drop tabella di output se esiste
DROP TABLE IF EXISTS job2_stats;

-- Crea la tabella finale con top 3 parole (in stringa separata da virgole)
CREATE TABLE job2_stats AS
WITH base AS (
    SELECT
        city,
        year,
        CASE
            WHEN price >= 50000 THEN 'alto'
            WHEN price >= 20000 THEN 'medio'
            ELSE 'basso'
        END AS fascia_prezzo,
        daysonmarket,
        SPLIT(description, ' ') AS parole
    FROM used_cars
),
explode_words AS (
    SELECT
        city,
        year,
        fascia_prezzo,
        daysonmarket,
        parola
    FROM base
    LATERAL VIEW explode(parole) exploded_table AS parola
    WHERE parola != ''
),
agg_words AS (
    SELECT
        city,
        year,
        fascia_prezzo,
        COUNT(*) AS num_annunci,
        AVG(daysonmarket) AS media_giorni,
        SORT_ARRAY(COLLECT_LIST(parola)) AS parole_ordinate
    FROM explode_words
    GROUP BY city, year, fascia_prezzo
)
SELECT
    city,
    year,
    fascia_prezzo,
    num_annunci,
    ROUND(media_giorni, 1) AS media_giorni,
    CONCAT_WS(',',
        parole_ordinate[0],
        parole_ordinate[1],
        parole_ordinate[2]
    ) AS top_3_words
FROM agg_words;
