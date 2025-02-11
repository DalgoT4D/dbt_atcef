{{ config(
  materialized='table',
  tags=["final","final_gdgs_union", "gdgs"]
) }}

select * from {{ref('lok_sahbag_gdgs_2023')}} 
UNION 
select * from {{ref('lok_sahbag_gdgs_2024')}} 