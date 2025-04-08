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
    endline_status,
    silt_achieved_by_endline_farmers,
    total_farm_area_silt_is_spread_on,
    silt_per_acre,
    silt_per_acre_benchmark_classification
from {{ ref('silt_per_acre_gdgs_2023') }}

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
    endline_status,
    silt_achieved_by_endline_farmers,
    total_farm_area_silt_is_spread_on,
    silt_per_acre,
    silt_per_acre_benchmark_classification
from {{ ref('silt_per_acre_gdgs_2024') }}
