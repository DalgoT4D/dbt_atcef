{{ config(
  materialized='table',
  tags=["final","final_niti_union", "niti"]
) }}

select * from {{ref('farmer_calc_silt_niti_22')}} 
UNION 
select * from {{ref('farmer_silt_calc_niti_2023')}} 
UNION 
select * from {{ref('farmer_calc_silt_niti_2024')}} 