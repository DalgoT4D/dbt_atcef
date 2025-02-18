{{ config(
  materialized='table',
  tags=["final","final_org"]
) }}

SELECT DISTINCT
    CAST(n.date_time AS DATE) AS date_time,
    n.work_order_name AS work_order_name,
    n.state AS state,
    n.district AS district,
    n.taluka AS taluka,
    n.village AS village,
    n.dam AS dam,
    n.ngo_name AS ngo_name,
    COALESCE(n.silt_achieved_by_endline_farmers, 0) AS silt_achieved_by_endline_farmers,
    n.total_farm_area_silt_is_spread_on,
    n.type_of_land_silt_is_spread_on,
    n.endline_status,
    'Niti Aayog' AS project
FROM {{ ref('barren_fertile_silt_niti_union') }} n

UNION

SELECT DISTINCT
    CAST(s.date_time AS DATE) AS date_time,
    s.work_order_name AS work_order_name,
    s.state AS state,
    s.district AS district,
    s.taluka AS taluka,
    s.village AS village,
    s.dam AS dam,
    s.ngo_name AS ngo_name,
    COALESCE(s.silt_achieved_by_endline_farmers, 0) AS silt_achieved_by_endline_farmers,
    s.total_farm_area_silt_is_spread_on,
    s.type_of_land_silt_is_spread_on,
    s.endline_status,
    'Project A' AS project
FROM {{ ref('barren_fertile_silt_gramin') }} s

UNION

SELECT DISTINCT
    CAST(g.date_time AS DATE) AS date_time,
    g.work_order_name AS work_order_name,
    g.state AS state,
    g.district AS district,
    g.taluka AS taluka,
    g.village AS village,
    g.dam AS dam,
    g.ngo_name AS ngo_name,
    COALESCE(g.silt_achieved_by_endline_farmers, 0) AS silt_achieved_by_endline_farmers,
    g.total_farm_area_silt_is_spread_on,
    g.type_of_land_silt_is_spread_on,
    g.endline_status,
    'GDGS' AS project
FROM {{ ref('barren_fertile_silt_gdgs_union') }} g