{{ config(
  materialized='table'
) }}


SELECT
    first_name AS work_order,
    state, 
    district,
    taluka,
    dam,
    village,
    COALESCE(silt_excavated_as_per_MB_recording, 0) AS silt_excavated_as_per_MB_recording,
    COALESCE(total_silt_excavated, 0) AS total_silt_excavated
FROM
    {{ ref('work_order_union') }}
WHERE
    encounter_type = 'Work order endline'
    AND project_ongoing = 'Ongoing'