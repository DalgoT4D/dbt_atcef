{{ config(
  materialized='table',
  tags=["final", "final_gdgs_union", "gdgs"]
) }}

{% set vulnerable_silt_columns = [
    'state',
    'district',
    'taluka',
    'village',
    'dam',
    'ngo_name',
    'farmer_type',
    'total_silt_carted'
] %}

SELECT
    {{ vulnerable_silt_columns | join(',\n    ') }}
FROM {{ ref('farmer_silt_vulnerable_gdgs_23') }}

UNION ALL

SELECT
    {{ vulnerable_silt_columns | join(',\n    ') }}
FROM {{ ref('farmer_silt_vulnerable_gdgs_24') }}

UNION ALL

SELECT
    {{ vulnerable_silt_columns | join(',\n    ') }}
FROM {{ ref('farmer_silt_vulnerable_gdgs_25') }}
