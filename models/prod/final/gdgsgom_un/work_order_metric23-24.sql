{{ config(
  materialized='table'
) }}

WITH aggregated_data AS (
    SELECT 
        first_name AS work_order,
        MAX(date_time) as date_time,
        state, 
        district,
        taluka,
        dam,
        village,
        MAX(CAST(silt_to_be_excavated_as_per_plan AS NUMERIC)) as silt_target,
        SUM(CAST(total_silt_carted AS NUMERIC)) as silt_achieved
    FROM {{ref('work_order_gdgs_union')}}
    WHERE
        encounter_type = 'Work order daily Recording - Farmer'
        AND project_ongoing = 'Ongoing' 
        AND silt_to_be_excavated_as_per_plan != 0 
    GROUP BY
        state, district, taluka, dam, village, first_name
)

SELECT *
FROM aggregated_data
WHERE silt_achieved IS NOT NULL
  AND silt_achieved::TEXT != 'NaN'
