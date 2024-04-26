{{ config(
  materialized='table'
) }}

WITH mb_data AS (
    SELECT 
        state AS a_state, 
        district AS a_district, 
        taluka AS a_taluka, 
        dam AS a_dam, 
        village AS a_village, 
        ngo_name AS a_ngo_name, 
        silt_excavated_as_per_mb_recording, 
        total_silt_excavated
    FROM prod_final.mb_recording_metric
),
wo_data AS (
    SELECT 
        state AS b_state, 
        district AS b_district, 
        taluka AS b_taluka, 
        dam AS b_dam, 
        village AS b_village, 
        ngo_name AS b_ngo_name, 
        silt_target, 
        silt_achieved
    FROM prod_aggregated.work_order_metric
)
SELECT
    COALESCE(mb.a_state, wo.b_state) AS state,
    COALESCE(mb.a_district, wo.b_district) AS district,
    COALESCE(mb.a_taluka, wo.b_taluka) AS taluka,
    COALESCE(mb.a_dam, wo.b_dam) AS dam,
    COALESCE(mb.a_village, wo.b_village) AS village,
    COALESCE(mb.a_ngo_name, wo.b_ngo_name, '-Unknown-') AS ngo_name,
    SUM(COALESCE(mb.silt_excavated_as_per_mb_recording, 0)) AS total_silt_excavated_as_per_mb_recording,
    SUM(COALESCE(mb.total_silt_excavated, 0)) AS total_silt_excavated,
    MAX(COALESCE(wo.silt_target, 0)) AS total_silt_target,
    MAX(COALESCE(wo.silt_achieved, 0)) AS total_silt_achieved
FROM mb_data mb
FULL OUTER JOIN wo_data wo
ON 
    mb.a_state = wo.b_state AND
    mb.a_district = wo.b_district AND
    mb.a_taluka = wo.b_taluka AND
    mb.a_dam = wo.b_dam AND
    mb.a_village = wo.b_village AND
    mb.a_ngo_name = wo.b_ngo_name
GROUP BY
    COALESCE(mb.a_state, wo.b_state),
    COALESCE(mb.a_district, wo.b_district),
    COALESCE(mb.a_taluka, wo.b_taluka),
    COALESCE(mb.a_dam, wo.b_dam),
    COALESCE(mb.a_village, wo.b_village),
    COALESCE(mb.a_ngo_name, wo.b_ngo_name, '-Unknown-')
ORDER BY
    COALESCE(mb.a_dam, wo.b_dam)
