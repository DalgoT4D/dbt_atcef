{{ config(
  materialized='table',
  tags=["final", "final_org"]
) }}

{% set columns = [
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

SELECT DISTINCT
    {{ columns | join(',\n    ') }},
    'Niti Aayog' AS project
FROM {{ ref('farmer_calc_silt_niti_union') }}

UNION

SELECT DISTINCT
    {{ columns | join(',\n    ') }},
    'Project A' AS project
FROM {{ ref('farmer_calc_silt_gramin_union') }}

UNION

SELECT DISTINCT
    {{ columns | join(',\n    ') }},
    'GDGS' AS project
FROM {{ ref('farmer_calc_silt_gdgs_union') }}
