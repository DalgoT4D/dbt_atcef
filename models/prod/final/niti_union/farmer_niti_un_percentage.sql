{{ config(
  materialized='table',
  tags=["final","final_niti_union", "niti"]
) }}


select
    state,
    district,
    taluka,
    village,
    dam,
    ngo_name,
    farmer_type,
    farmers_count
from {{ ref('farmer_niti_2022_percentage') }}

union all

select
    state,
    district,
    taluka,
    village,
    dam,
    ngo_name,
    farmer_type,
    farmers_count
from {{ ref('farmer_niti_2023_percentage') }}

union all

select
    state,
    district,
    taluka,
    village,
    dam,
    ngo_name,
    farmer_type,
    farmers_count
from {{ ref('farmer_niti_2024_percentage') }}

union all

select
    state,
    district,
    taluka,
    village,
    dam,
    ngo_name,
    farmer_type,
    farmers_count
from {{ ref('farmer_niti_2025_percentage') }}
