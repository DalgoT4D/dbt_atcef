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
    silt_achieved_by_endline_farmers,
    total_farm_area_silt_is_spread_on,
    type_of_land_silt_is_spread_on,
    endline_status
from {{ ref('barren_fertile_silt_niti_22') }}

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
    silt_achieved_by_endline_farmers,
    total_farm_area_silt_is_spread_on,
    type_of_land_silt_is_spread_on,
    endline_status
from {{ ref('barren_fertile_silt_niti_2023') }}

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
    silt_achieved_by_endline_farmers,
    total_farm_area_silt_is_spread_on,
    type_of_land_silt_is_spread_on,
    endline_status
from {{ ref('barren_fertile_silt_niti_2024') }}
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
    silt_achieved_by_endline_farmers,
    total_farm_area_silt_is_spread_on,
    type_of_land_silt_is_spread_on,
    endline_status
from {{ ref('barren_fertile_silt_niti_2025') }}
