{{ config(
  materialized='table',
  tags=["final","final_gdgs_union", "gdgs"]
) }}

select 
    date_time,
    work_order_name,
    state,
    district,
    taluka,
    village,
    dam,
    ngo_name,
    silt_target,
    silt_achieved,
    total_farm_area_silt_is_spread_on
from {{ref('work_order_metric_gdgs_23')}}

union all 

select 
    date_time,
    work_order_name,
    state,
    district,
    taluka,
    village,
    dam,
    ngo_name,
    silt_target,
    silt_achieved,
    total_farm_area_silt_is_spread_on
from {{ref('work_order_metric_gdgs_24')}}