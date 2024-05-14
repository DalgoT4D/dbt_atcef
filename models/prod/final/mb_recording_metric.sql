{{ config(
  materialized='table'
) }}


SELECT
    distinct on (eid)
    eid,
    date_time,
    first_name AS work_order,
    state, 
    district,
    taluka,
    dam,
    village,
    ngo_name,
    COALESCE(silt_excavated_as_per_MB_recording, 0) AS silt_excavated_as_per_MB_recording,
    COALESCE(total_silt_carted, 0) AS total_silt_excavated
FROM
    {{ ref('work_order_union') }}
