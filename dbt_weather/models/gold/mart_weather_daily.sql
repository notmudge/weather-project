{{ config(materialized="table") }}

SELECT
    location_name,
    observation_dt,
    ROUND(AVG(temperature_c), 1)        AS temp_avg_c,
    ROUND(MIN(temperature_c), 1)        AS temp_min_c,
    ROUND(MAX(temperature_c), 1)        AS temp_max_c,
    ROUND(AVG(apparent_temp_c), 1)      AS feels_like_avg_c,
    ROUND(SUM(precipitation_mm), 2)     AS total_precip_mm,
    ROUND(SUM(rain_mm), 2)              AS total_rain_mm,
    ROUND(SUM(snowfall_cm), 2)          AS total_snow_cm,
    ROUND(AVG(windspeed_kmh), 1)        AS wind_avg_kmh,
    ROUND(MAX(windgusts_kmh), 1)        AS wind_max_gust_kmh,
    ROUND(AVG(cloudcover_pct), 1)       AS cloud_avg_pct,
    ROUND(AVG(humidity_pct), 1)         AS humidity_avg_pct,
    CASE WHEN SUM(precipitation_mm) > 1 THEN 1 ELSE 0 END AS is_rain_day,
    CASE WHEN MIN(temperature_c) < 0    THEN 1 ELSE 0 END AS is_frost_day,
    CASE WHEN SUM(snowfall_cm) > 0      THEN 1 ELSE 0 END AS is_snow_day,
    COUNT(*) AS hours_with_data
FROM {{ ref("stg_weather_hourly") }}
GROUP BY location_name, observation_dt
HAVING COUNT(*) >= 20
