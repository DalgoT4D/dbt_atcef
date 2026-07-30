{{ config(
  materialized='table',
  tags=["final", "final_gramin_union", "gramin_niti"]
) }}

{% set work_order_metric_columns = [
    'work_order_name',
    'state',
    'district',
    'taluka',
    'dam',
    'ngo_name',
    'village',
    'date_time',
    'silt_target',
    'silt_achieved',
    'total_farm_area_silt_is_spread_on'
] %}

SELECT
    {{ work_order_metric_columns | join(',\n    ') }}
FROM {{ ref('work_order_silt_calc') }}

UNION ALL

SELECT
    {{ work_order_metric_columns | join(',\n    ') }}
FROM {{ ref('work_order_silt_calc_25') }}

UNION ALL

SELECT
    {{ work_order_metric_columns | join(',\n    ') }}
FROM {{ ref('ss_work_order_metric_graminpa_26') }}
