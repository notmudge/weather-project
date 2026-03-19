

WITH source AS (

    SELECT
        LOWER(TRIM(location_name))             AS location_name,
        CAST(observation_dt AS DATE)           AS observation_dt,
        CAST(hour_utc AS TINYINT)              AS hour_utc,
        CAST(temperature_2m AS FLOAT)          AS temperature_c,
        CAST(apparent_temp AS FLOAT)           AS apparent_temp_c,
        CAST(precipitation AS FLOAT)           AS precipitation_mm,
        CAST(rain AS FLOAT)                    AS rain_mm,
        CAST(snowfall AS FLOAT)                AS snowfall_cm,
        CAST(windspeed_10m AS FLOAT)           AS windspeed_kmh,
        CAST(windgusts_10m AS FLOAT)           AS windgusts_kmh,
        CAST(cloudcover AS FLOAT)              AS cloudcover_pct,
        CAST(weathercode AS INT)               AS wmo_weathercode,
        CAST(humidity_2m AS FLOAT)             AS humidity_pct,
        CAST(ingested_at AS DATE)              AS ingested_at

    FROM "free-sql-db-5344531"."dbo"."bronze_weather_raw"

    WHERE
        observation_dt IS NOT NULL
        AND hour_utc BETWEEN 0 AND 23
        AND temperature_2m BETWEEN -80 AND 60

    

)

SELECT * FROM source