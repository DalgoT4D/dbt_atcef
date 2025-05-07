{{ config(
  materialized='table',
  tags=["final","final_niti_union", "niti"]
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
from {{ ref('silt_per_acre_niti_22') }}

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
from {{ ref('silt_per_acre_niti_23') }}

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
from {{ ref('silt_per_acre_niti_2024') }}

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
from {{ ref('silt_per_acre_niti_2025') }}
