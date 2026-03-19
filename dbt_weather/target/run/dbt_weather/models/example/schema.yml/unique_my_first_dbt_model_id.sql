
    
    -- Create target schema if it does not
  USE [free-sql-db-5344531];
  IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'dbt_dev')
  BEGIN
    EXEC('CREATE SCHEMA [dbt_dev]')
  END

  

  
  EXEC('create view 
    [dbt_dev].[testview_09bfc1f6e2654cb7c3f5b282c4ea0d12_15387]
   as 
    
    
    

select
    id as unique_field,
    count(*) as n_records

from "free-sql-db-5344531"."dbt_dev"."my_first_dbt_model"
where id is not null
group by id
having count(*) > 1



  ;')
  select
    
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from (
    select * from 
    [dbt_dev].[testview_09bfc1f6e2654cb7c3f5b282c4ea0d12_15387]
  
  ) dbt_internal_test;

  EXEC('drop view 
    [dbt_dev].[testview_09bfc1f6e2654cb7c3f5b282c4ea0d12_15387]
  ;')