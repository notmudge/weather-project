# Weather Data Pipeline

A production-like end-to-end data engineering pipeline that ingests hourly weather data for 5 UK cities, transforms it through a medallion architecture, and serves aggregated climate analytics.

## Tech stack

| Layer | Technology | Purpose |
|---|---|---|
| Ingestion | Python 3.11 | API calls, blob uploads |
| Source API | Open-Meteo | Free historical weather data, no key required |
| Data Lake | Azure Data Lake Storage Gen2 | Raw JSON storage, date-partitioned |
| Data Warehouse | Azure SQL Database | Staging, Silver, and Gold layers |
| Transformation | dbt Core + dbt-sqlserver | SQL models, incremental loads, tests, docs |
| Orchestration | GitHub Actions | Scheduled daily cron, workflow chaining |
| Version Control | GitHub | Source control, CI/CD, secrets management |
| Development | GitHub Codespaces | Cloud development environment |

## Architecture
```
Open-Meteo API → ADLS Gen2 (Bronze) → Azure SQL Staging → dbt Silver → dbt Gold
                                                                ↑
                                                       GitHub Actions (daily cron)
```

## Medallion layers

**Bronze** — Raw hourly JSON files landed in ADLS Gen2, partitioned as `weather/year=YYYY/month=MM/day=DD/city.json`. No transformation, append-only.

**Silver** — `stg_weather_hourly` — cleaned, typed incremental model in Azure SQL. Filters out nulls and out-of-range values. One row per city per hour.

**Gold** — Two mart tables in Azure SQL, rebuilt daily:
- `mart_weather_daily` — temperature range, precipitation totals, wind speeds, and derived flags (`is_rain_day`, `is_frost_day`, `is_snow_day`)
- `mart_weather_monthly` — rolled up from daily mart, includes frost/rain/snow day counts and monthly climate averages

## Cities covered

London · Manchester · Edinburgh · Birmingham · Cardiff

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

## Requirements

- Azure for Students subscription (free — no credit card required)
- GitHub account with Codespaces enabled
- No paid API keys required