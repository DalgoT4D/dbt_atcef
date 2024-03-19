{{ config(
  materialized='table'
) }}

SELECT 
    dam, 
    max(date_time) as date_time, 
    district, 
    state, 
    taluka, 
    village, 
    max(silt_to_be_excavated) as silt_target, 
    sum(total_silt_carted) as silt_achieved,
    COUNT(CASE WHEN mobile_verified = TRUE THEN 1 END) AS mobile_verified_count,
    COUNT(CASE WHEN mobile_verified = FALSE THEN 1 END) AS mobile_unverified_count,
    COUNT(CASE WHEN project_status = 'Ongoing' THEN 1 END) AS ongoing_projects_count,
    COUNT(CASE WHEN project_status = 'Not Started' THEN 1 END) AS not_started_projects_count,
    -- Concatenating the '%' sign after rounding the value
    ROUND((SUM(total_silt_carted)::numeric / NULLIF(MAX(silt_to_be_excavated), 0)::numeric), 2)  AS "silt_achieved in %",
    ROUND(SUM(total_silt_carted) * 1000) AS additional_surface_water,
    ROUND((SUM(total_silt_carted) * 1000) / 10000) AS equivalent_water_tankers
FROM
    {{ ref('work_order_union') }} 
WHERE
    encounter_type = 'Work order daily Recording - Farmer'
GROUP BY
    state, district, taluka, village, dam
