{{ config(
  materialized='table'
) }}

SELECT 
    dam, 
    max(date_time) as date, 
    district, 
    state, 
    taluka, 
    village, 
    max(silt_to_be_excavated) as silt_target, 
    sum(total_silt_carted) as silt_achieved,
    ROUND((SUM(total_silt_carted)::numeric / NULLIF(MAX(silt_to_be_excavated), 0)::numeric), 2) AS pct_silt_achieved_vs_target,
    -- Adjusted calculations for aggregate values
    (MAX(silt_to_be_excavated) - SUM(total_silt_carted)) * 1000 AS silt_difference_litres,
    (MAX(silt_to_be_excavated) * 1000) / 10000 AS equivalent_water_tankers
FROM
    {{ ref('work_order_union') }} 
WHERE
    encounter_type = 'Work order daily Recording - Farmer'
GROUP BY
    dam, district, state, taluka, village
