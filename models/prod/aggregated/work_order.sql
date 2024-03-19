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
    ROUND((SUM(total_silt_carted)::numeric / NULLIF(MAX(silt_to_be_excavated), 0)::numeric), 2)  AS "silt_achieved in %",
    ROUND(SUM(total_silt_carted) * 1000) AS additional_surface_water,
    ROUND((SUM(total_silt_carted) * 1000) / 10000) AS equivalent_water_tankers
FROM
    {{ ref('work_order_union') }} 
WHERE
   dam is not null
GROUP BY
    state, district, taluka, village, dam, project_status
