{{ config(
  materialized='table'
) }}

select * from {{ref('farmer_calc_silt_gdgs_23')}} 
UNION 
select * from {{ref('farmer_calc_silt_gdgs_24')}} 