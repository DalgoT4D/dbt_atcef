{{ config(
  materialized='table'
) }}


SELECT 
    MAX(date_time) as date_time,
    state, 
    district,
    taluka,
    dam,
    village,
    MAX(silt_to_be_excavated) AS silt_target,
    SUM(CASE WHEN total_silt_carted::TEXT <> 'NaN' THEN total_silt_carted ELSE 0 END) AS silt_achieved
    FROM
    {{ ref('work_order_union') }}
WHERE
    encounter_type = 'Work order daily Recording - Farmer'
    AND project_ongoing = 'Ongoing'
GROUP BY
    state, district, taluka, dam, village