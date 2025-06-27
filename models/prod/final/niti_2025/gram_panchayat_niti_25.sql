{{ config(
  materialized='table',
  tags=["final","final_niti_2025", "niti_2025", "niti"]
) }}

select
    date_time,
    first_name as work_order_name,
    state,
    district,
    taluka,
    village,
    ngo_name,
    cast(
        total_silt_excavated_by_gp_for_non_farm_purpose as numeric
    ) as total_silt_excavated_by_gp_for_non_farm_purpose
from {{ ref('work_order_2025_niti') }}
where
    encounter_type = 'Gram Panchayat Endline'
    and cast(total_silt_excavated_by_gp_for_non_farm_purpose as numeric) != 0 