
    
    -- Create target schema if it does not
  USE [free-sql-db-5344531];
  IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'dbt_dev')
  BEGIN
    EXEC('CREATE SCHEMA [dbt_dev]')
  END

  

  
  EXEC('create view 
    [dbt_dev].[testview_93f97fdf737f03a68627b94b6bd3567e_5728]
   as 
    
    
    



select id
from "free-sql-db-5344531"."dbt_dev"."my_second_dbt_model"
where id is null



  ;')
  select
    
    count(*) as failures,
    case when count(*) != 0
      then 'true' else 'false' end as should_warn,
    case when count(*) != 0
      then 'true' else 'false' end as should_error
  from (
    select * from 
    [dbt_dev].[testview_93f97fdf737f03a68627b94b6bd3567e_5728]
  
  ) dbt_internal_test;

  EXEC('drop view 
    [dbt_dev].[testview_93f97fdf737f03a68627b94b6bd3567e_5728]
  ;')