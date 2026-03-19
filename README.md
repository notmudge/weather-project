# Weather Data Pipeline

A production-like end-to-end data engineering pipeline that ingests hourly weather data for 5 UK cities, transforms it through a medallion architecture, and serves aggregated climate analytics.

## Stack

- **Ingestion** — Python + Open-Meteo API (free, no key required)
- **Bronze** — Raw JSON stored in Azure Data Lake Storage Gen2, partitioned by date
- **Silver** — Cleaned and typed hourly fact table in Azure SQL (dbt incremental model)
- **Gold** — Aggregated daily and monthly mart tables (dbt table models)
- **Orchestration** — GitHub Actions, scheduled daily at 06:00 UTC
- **Transformation** — dbt Core with tests and documentation

## Architecture
```
Open-Meteo API → ADLS Gen2 (Bronze) → Azure SQL Staging → dbt Silver → dbt Gold
                                                                ↑
                                                       GitHub Actions (daily cron)
```

## Cities covered

London · Manchester · Edinburgh · Birmingham · Cardiff

## Data model

**Silver — `stg_weather_hourly`**
One row per city per hour. Cleaned, typed, and validated from raw Bronze JSON.

**Gold — `mart_weather_daily`**
One row per city per day. Includes temperature range, precipitation totals, wind speeds, and derived flags: `is_rain_day`, `is_frost_day`, `is_snow_day`.

**Gold — `mart_weather_monthly`**
One row per city per month. Rolled up from the daily mart. Includes frost day counts, rain day counts, monthly precipitation totals, and average temperatures.

## Pipeline flow

1. `ingest_weather.py` — fetches yesterday's hourly data from Open-Meteo and uploads raw JSON to ADLS Gen2
2. `load_bronze_to_sql.py` — reads blobs from ADLS and merges rows into `bronze_weather_raw` in Azure SQL
3. `dbt run` — runs Silver and Gold models, incremental load for Silver
4. `dbt test` — validates not_null constraints and accepted value ranges

## Automation

Two GitHub Actions workflows:
- **Daily ingestion** (`ingest.yml`) — runs at 06:00 UTC, triggers the Python ingestion and SQL load
- **dbt transform** (`dbt_run.yml`) — triggers automatically on successful ingestion completion

## Sample queries
```sql
-- Frost days in London Q1 2024
SELECT SUM(is_frost_day) AS frost_days
FROM dbt_prod.mart_weather_daily
WHERE location_name = 'london'
AND observation_dt BETWEEN '2024-01-01' AND '2024-03-31';

-- Wettest months across all cities
SELECT location_name, year_month, total_precip_mm
FROM dbt_prod.mart_weather_monthly
ORDER BY total_precip_mm DESC;

-- Average temperature by city, summer 2024
SELECT location_name, ROUND(AVG(temp_avg_c), 1) AS avg_temp
FROM dbt_prod.mart_weather_daily
WHERE observation_dt BETWEEN '2024-06-01' AND '2024-08-31'
GROUP BY location_name
ORDER BY avg_temp DESC;
```

## Setup

See full setup guide for Azure infrastructure, dbt configuration, and GitHub Secrets required to run this pipeline.