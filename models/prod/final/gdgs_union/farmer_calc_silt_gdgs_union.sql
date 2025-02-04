{{ config(
  materialized='table',
  tags=["final","final_gdgs_union"]
) }}

select * from {{ref('farmer_calc_silt_gdgs_23')}} 
UNION 
select * from {{ref('farmer_calc_silt_gdgs_24')}} 