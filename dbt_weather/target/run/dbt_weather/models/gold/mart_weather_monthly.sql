
  
    USE [free-sql-db-5344531];
    USE [free-sql-db-5344531];
    
    

    

    
    USE [free-sql-db-5344531];
    EXEC('
        create view "dbt_dev_gold"."mart_weather_monthly__dbt_tmp__dbt_tmp_vw" as 

SELECT
    location_name,
    YEAR(observation_dt)                    AS year,
    MONTH(observation_dt)                   AS month,
    CAST(YEAR(observation_dt) AS VARCHAR) + ''-'' + RIGHT(''0'' + CAST(MONTH(observation_dt) AS VARCHAR), 2) AS year_month,
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
FROM "free-sql-db-5344531"."dbt_dev_gold"."mart_weather_daily"
GROUP BY
    location_name,
    YEAR(observation_dt),
    MONTH(observation_dt);
    ')

EXEC('
            SELECT * INTO "free-sql-db-5344531"."dbt_dev_gold"."mart_weather_monthly__dbt_tmp" FROM "free-sql-db-5344531"."dbt_dev_gold"."mart_weather_monthly__dbt_tmp__dbt_tmp_vw" 
    OPTION (LABEL = ''dbt-sqlserver'');

        ')

    
    EXEC('DROP VIEW IF EXISTS dbt_dev_gold.mart_weather_monthly__dbt_tmp__dbt_tmp_vw')



    
    use [free-sql-db-5344531];
    if EXISTS (
        SELECT *
        FROM sys.indexes with (nolock)
        WHERE name = 'dbt_dev_gold_mart_weather_monthly__dbt_tmp_cci'
        AND object_id=object_id('dbt_dev_gold_mart_weather_monthly__dbt_tmp')
    )
    DROP index "dbt_dev_gold"."mart_weather_monthly__dbt_tmp".dbt_dev_gold_mart_weather_monthly__dbt_tmp_cci
    CREATE CLUSTERED COLUMNSTORE INDEX dbt_dev_gold_mart_weather_monthly__dbt_tmp_cci
    ON "dbt_dev_gold"."mart_weather_monthly__dbt_tmp"

   


  