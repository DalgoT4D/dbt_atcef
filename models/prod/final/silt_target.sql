{{ config(
  materialized='table'
) }}


SELECT 
    date_time,
    state,
    district,
    taluka,
    village,
    dam as waterbodies,
    ngo_name,
    'silt_target' AS metric_type,
    silt_target AS metric_value
FROM 
    {{ref('work_order_metric')}}

UNION ALL

SELECT 
    date_time,
    state,
    district,
    taluka,
    village,
    dam as waterbodies,
    ngo_name,
    'silt_achieved' AS metric_type,
    silt_achieved AS metric_value
FROM 
    {{ref('work_order_metric')}}
ORDER BY
    date_time,
    waterbodies,
    metric_type
