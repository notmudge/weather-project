
    
    -- Create target schema if it does not
  USE [free-sql-db-5344531];
  IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'dbt_dev')
  BEGIN
    EXEC('CREATE SCHEMA [dbt_dev]')
  END

  

  
  EXEC('create view 
    [dbt_dev].[testview_e610a2123f6d23f50e66d3813246ffd1_8182]
   as 
    
    
    



select temperature_c
from "free-sql-db-5344531"."dbt_dev_silver"."stg_weather_hourly"
where temperature_c is null



  ;')
  select
    
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from (
    select * from 
    [dbt_dev].[testview_e610a2123f6d23f50e66d3813246ffd1_8182]
  
  ) dbt_internal_test;

  EXEC('drop view 
    [dbt_dev].[testview_e610a2123f6d23f50e66d3813246ffd1_8182]
  ;')