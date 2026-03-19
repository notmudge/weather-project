
    
    -- Create target schema if it does not
  USE [free-sql-db-5344531];
  IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'dbt_dev')
  BEGIN
    EXEC('CREATE SCHEMA [dbt_dev]')
  END

  

  
  EXEC('create view 
    [dbt_dev].[testview_479a7a1bcef5268927ee8a25c5641c4a_15862]
   as 
    
    
    



select location_name
from "free-sql-db-5344531"."dbt_dev_silver"."stg_weather_hourly"
where location_name is null



  ;')
  select
    
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from (
    select * from 
    [dbt_dev].[testview_479a7a1bcef5268927ee8a25c5641c4a_15862]
  
  ) dbt_internal_test;

  EXEC('drop view 
    [dbt_dev].[testview_479a7a1bcef5268927ee8a25c5641c4a_15862]
  ;')