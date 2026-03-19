
      
  
    USE [free-sql-db-5344531];
    USE [free-sql-db-5344531];
    
    

    

    
    USE [free-sql-db-5344531];
    EXEC('
        create view "dbt_dev_silver"."stg_weather_hourly__dbt_tmp_vw" as 

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

SELECT * FROM source;
    ')

EXEC('
            SELECT * INTO "free-sql-db-5344531"."dbt_dev_silver"."stg_weather_hourly" FROM "free-sql-db-5344531"."dbt_dev_silver"."stg_weather_hourly__dbt_tmp_vw" 
    OPTION (LABEL = ''dbt-sqlserver'');

        ')

    
    EXEC('DROP VIEW IF EXISTS dbt_dev_silver.stg_weather_hourly__dbt_tmp_vw')



    
    use [free-sql-db-5344531];
    if EXISTS (
        SELECT *
        FROM sys.indexes with (nolock)
        WHERE name = 'dbt_dev_silver_stg_weather_hourly_cci'
        AND object_id=object_id('dbt_dev_silver_stg_weather_hourly')
    )
    DROP index "dbt_dev_silver"."stg_weather_hourly".dbt_dev_silver_stg_weather_hourly_cci
    CREATE CLUSTERED COLUMNSTORE INDEX dbt_dev_silver_stg_weather_hourly_cci
    ON "dbt_dev_silver"."stg_weather_hourly"

   


  
  