{{ config(
  materialized='table'
) }}


SELECT 
    first_name as work_order,
    MAX(date_time) as date_time,
    ngo_name,
    state, 
    district,
    taluka,
    dam,
    village,
    COALESCE(MAX(silt_target), 0) as silt_target, 
    COALESCE(CAST(ROUND(SUM(CASE WHEN total_silt_carted::text <> 'NaN' THEN total_silt_carted ELSE 0 END)) AS numeric), 0) AS silt_achieved
    FROM
    {{ ref('work_order_union') }}
WHERE
    encounter_type = 'Work order daily Recording - Farmer'
    AND project_ongoing = 'Ongoing' and silt_target != 0
GROUP BY
    state, district, taluka, dam, village, first_name, ngo_name