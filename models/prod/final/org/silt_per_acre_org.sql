{{ config(
  materialized='table',
  tags=["final","final_org"]
) }}

SELECT DISTINCT
    CAST(n.date_time AS DATE) AS date_time,
    n.work_order_name,
    n.state,
    n.district,
    n.taluka,
    n.village,
    n.dam,
    n.ngo_name,
    n.endline_status,
    COALESCE(
        n.silt_achieved_by_endline_farmers, 0
    ) AS silt_achieved_by_endline_farmers,
    n.total_farm_area_silt_is_spread_on,
    n.silt_per_acre,
    n.silt_per_acre_benchmark_classification,
    'Niti Aayog' AS project
FROM {{ ref('silt_per_acre_niti_union') }} AS n

UNION

SELECT DISTINCT
    CAST(s.date_time AS DATE) AS date_time,
    s.work_order_name,
    s.state,
    s.district,
    s.taluka,
    s.village,
    s.dam,
    s.ngo_name,
    s.endline_status,
    COALESCE(
        s.silt_achieved_by_endline_farmers, 0
    ) AS silt_achieved_by_endline_farmers,
    s.total_farm_area_silt_is_spread_on,
    s.silt_per_acre,
    s.silt_per_acre_benchmark_classification,
    'Project A' AS project
FROM {{ ref('silt_per_acre_gramin') }} AS s

UNION

SELECT DISTINCT
    CAST(g.date_time AS DATE) AS date_time,
    g.work_order_name,
    g.state,
    g.district,
    g.taluka,
    g.village,
    g.dam,
    g.ngo_name,
    g.endline_status,
    COALESCE(
        g.silt_achieved_by_endline_farmers, 0
    ) AS silt_achieved_by_endline_farmers,
    g.total_farm_area_silt_is_spread_on,
    g.silt_per_acre,
    g.silt_per_acre_benchmark_classification,
    'GDGS' AS project
FROM {{ ref('silt_per_acre_gdgs_union') }} AS g
