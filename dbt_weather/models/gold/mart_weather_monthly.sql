{{ config(materialized="table") }}

SELECT
    location_name,
    YEAR(observation_dt)                    AS year,
    MONTH(observation_dt)                   AS month,
    CAST(YEAR(observation_dt) AS VARCHAR) + '-' + RIGHT('0' + CAST(MONTH(observation_dt) AS VARCHAR), 2) AS year_month,
    COUNT(*)                                AS days_with_data,
    ROUND(AVG(temp_avg_c), 1)              AS monthly_avg_temp_c,
    ROUND(MIN(temp_min_c), 1)              AS monthly_min_temp_c,
    ROUND(MAX(temp_max_c), 1)              AS monthly_max_temp_c,
    ROUND(SUM(total_precip_mm), 1)         AS total_precip_mm,
    SUM(is_rain_day)                        AS rain_days,
    SUM(is_frost_day)                       AS frost_days,
    SUM(is_snow_day)                        AS snow_days,
    ROUND(AVG(wind_avg_kmh), 1)            AS avg_wind_kmh,
    ROUND(MAX(wind_max_gust_kmh), 1)       AS max_gust_kmh,
    ROUND(AVG(cloud_avg_pct), 1)           AS avg_cloudcover_pct
FROM {{ ref("mart_weather_daily") }}
GROUP BY
    location_name,
    YEAR(observation_dt),
    MONTH(observation_dt)
