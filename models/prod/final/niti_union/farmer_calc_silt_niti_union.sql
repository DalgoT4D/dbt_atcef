{{ config(
  materialized='table',
  tags=["final", "final_niti_union", "niti"]
) }}

{% set farmer_silt_columns = [
    'eid',
    'farmer_id',
    'work_order_id',
    'state',
    'village',
    'machine_sub_id',
    'district',
    'taluka',
    'dam',
    'ngo_name',
    'farmer_name',
    'mobile_number',
    'mobile_verified',
    'category_of_farmer',
    'work_order_name',
    'silt_target',
    'total_silt_carted',
    'date_time'
] %}

SELECT
    {{ farmer_silt_columns | join(',\n    ') }}
FROM {{ ref('farmer_calc_silt_niti_22') }}

UNION ALL

SELECT
    {{ farmer_silt_columns | join(',\n    ') }}
FROM {{ ref('farmer_silt_calc_niti_2023') }}

UNION ALL

SELECT
    {{ farmer_silt_columns | join(',\n    ') }}
FROM {{ ref('farmer_calc_silt_niti_2024') }}

UNION ALL

SELECT
    {{ farmer_silt_columns | join(',\n    ') }}
FROM {{ ref('farmer_calc_silt_niti_2025') }}
