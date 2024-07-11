{{ config(
  materialized='table'
) }}

select 
    date_time,
    work_order_name,
    state,
    district,
    taluka,
    village,
    dam,
    silt_target,
    silt_achieved
from {{ref('work_order_metric_niti_23')}}

union all 

select 
    date_time,
    work_order_name,
    state,
    district,
    taluka,
    village,
    dam,
    silt_target,
    silt_achieved
from {{ref('work_order_metric_niti_22')}}

union all 

select 
    date_time,
    work_order_name,
    state,
    district,
    taluka,
    village,
    dam,
    silt_target,
    silt_achieved
from {{ref('work_order_metric_niti_2024')}}