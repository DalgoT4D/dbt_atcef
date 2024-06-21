{{ config(
  materialized='table'
) }}

select * from {{ref('lok_sahbag_gdgs_2023')}} 
UNION 
select * from {{ref('lok_sahbag_gdgs_2024')}} 