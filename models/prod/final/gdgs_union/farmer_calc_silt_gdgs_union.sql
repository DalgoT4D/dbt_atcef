{{ config(
  materialized='table',
  tags=["final","final_gdgs_union", "gdgs"]
) }}

select * from {{ ref('farmer_calc_silt_gdgs_23') }}
union
select * from {{ ref('farmer_calc_silt_gdgs_24') }}
union
select * from {{ ref('farmer_calc_silt_gdgs_25') }}
