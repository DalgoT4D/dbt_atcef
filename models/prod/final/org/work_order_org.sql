{{ config(
  materialized='table'
) }}

WITH enriched_encounters AS (
    SELECT 
        e.eid AS eid,
        e.date_time AS date_time,
        e.state AS state,
        e.district AS district,
        e.taluka AS taluka,
        NULL AS dam,
        e.village AS village,
        e.encounter_type AS encounter_type,
        e.total_working_hours_of_machine AS total_working_hours_of_machine,
        e.silt_excavated_as_per_mb_recording AS silt_excavated_as_per_mb_recording,
        e.total_silt_carted AS total_silt_carted,
        e.total_farm_area AS total_farm_area,
        e.area_covered_by_silt AS area_covered_by_silt,
        e.number_of_trolleys_carted AS number_of_trolleys_carted,
        e.total_farm_area_on_which_silt_is_spread AS total_farm_area_on_which_silt_is_spread,
        e.approval_status AS approval_status,
        'Project A' AS project
    FROM {{ ref('encounters_gramin') }} e

),

niti_data AS (
    SELECT DISTINCT
        n.eid AS eid,
        n.date_time AS date_time,
        n.state AS state,
        n.district AS district,
        n.taluka AS taluka,
        n.dam AS dam,
        n.village AS village,
        n.encounter_type AS encounter_type,
        n.total_working_hours_of_machine AS total_working_hours_of_machine,
        n.silt_excavated_as_per_mb_recording AS silt_excavated_as_per_mb_recording,
        n.total_silt_carted AS total_silt_carted,
        n.total_farm_area AS total_farm_area,
        n.area_covered_by_silt AS area_covered_by_silt,
        n.number_of_trolleys_carted AS number_of_trolleys_carted,
        n.total_farm_area_on_which_silt_is_spread AS total_farm_area_on_which_silt_is_spread,
        n.approval_status AS approval_status,
        'Niti Aayog' AS project
    FROM {{ ref('work_order_niti_union') }} n
),

gdgs_data AS (
    SELECT DISTINCT
        g.eid AS eid,
        g.date_time AS date_time,
        g.state AS state,
        g.district AS district,
        g.taluka AS taluka,
        g.dam AS dam,
        g.village AS village,
        g.encounter_type AS encounter_type,
        g.total_working_hours_of_machine AS total_working_hours_of_machine,
        g.silt_excavated_as_per_mb_recording AS silt_excavated_as_per_mb_recording,
        g.total_silt_carted AS total_silt_carted,
        g.total_farm_area AS total_farm_area,
        g.area_covered_by_silt AS area_covered_by_silt,
        g.number_of_trolleys_carted AS number_of_trolleys_carted,
        g.total_farm_area_on_which_silt_is_spread AS total_farm_area_on_which_silt_is_spread,
        g.approval_status AS approval_status,
        'GDGS' AS project
    FROM {{ ref('work_order_gdgs_union') }} g
)

SELECT * FROM enriched_encounters

UNION

SELECT * FROM niti_data

UNION

SELECT * FROM gdgs_data