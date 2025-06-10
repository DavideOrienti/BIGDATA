USE car_data;

-- Drop tabella di output se esiste
DROP TABLE IF EXISTS job2_stats;

-- Crea una vista temporanea con categoria di prezzo e parole
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
    LATERAL VIEW explode(parole) AS parola
    WHERE parola != ''
),
agg_words AS (
    SELECT
        city,
        year,
        fascia_prezzo,
        COUNT(*) AS num_annunci,
        AVG(daysonmarket) AS media_giorni,
        COLLECT_LIST(parola) AS tutte_parole
    FROM explode_words
    GROUP BY city, year, fascia_prezzo
)

-- Crea la tabella finale con top 3 parole (in stringa separata da virgole)
SELECT
    city,
    year,
    fascia_prezzo,
    num_annunci,
    ROUND(media_giorni, 1) AS media_giorni,
    CONCAT_WS(',',
        COLLECT_LIST(p)[0],
        COLLECT_LIST(p)[1],
        COLLECT_LIST(p)[2]
    ) AS top_3_words
FROM (
    SELECT
        city,
        year,
        fascia_prezzo,
        num_annunci,
        media_giorni,
        SORT_ARRAY(COLLECT_LIST(parola)) AS parole_ordinate
    FROM (
        SELECT
            city,
            year,
            fascia_prezzo,
            num_annunci,
            media_giorni,
            parola
        FROM agg_words
        LATERAL VIEW explode(tutte_parole) AS parola
    ) sub1
    GROUP BY city, year, fascia_prezzo, num_annunci, media_giorni
) sub2
LATERAL VIEW posexplode(parole_ordinate) expl AS pos, p
WHERE pos < 3
GROUP BY city, year, fascia_prezzo, num_annunci, media_giorni;
