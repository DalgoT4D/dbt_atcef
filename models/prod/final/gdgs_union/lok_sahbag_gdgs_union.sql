{{ config(
  materialized='table',
  tags=["final", "final_gdgs_union", "gdgs"]
) }}

{% set lok_sahbag_columns = [
    'lok_sahbhag_id',
    'lok_sahbhag_voided',
    'lok_sahbhag_name',
    'district',
    'state',
    'end_date',
    'image_after_work',
    'image_before_work',
    'image_during_work',
    'taluka',
    'village',
    'dam',
    'start_date',
    'total_silt_excavated',
    'lok_sahbhag_type',
    'village_code',
    'date_time',
    'ngo_name'
] %}

SELECT
    {{ lok_sahbag_columns | join(',\n    ') }}
FROM {{ ref('lok_sahbag_gdgs_2023') }}

UNION ALL

SELECT
    {{ lok_sahbag_columns | join(',\n    ') }}
FROM {{ ref('lok_sahbag_gdgs_2024') }}

UNION ALL

SELECT
    {{ lok_sahbag_columns | join(',\n    ') }}
FROM {{ ref('lok_sahbag_gdgs_2025') }}
