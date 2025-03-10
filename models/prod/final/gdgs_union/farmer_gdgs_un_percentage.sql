{{ config(
  materialized='table',
  tags=["final","final_gdgs_union", "gdgs"]
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
from {{ ref('farmer_gdgs_2023_percentage') }}

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
from {{ ref('farmer_gdgs_2024_percentage') }}
