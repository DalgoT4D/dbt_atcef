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
    COALESCE(n.silt_target, 0) AS silt_target,
    COALESCE(n.silt_achieved, 0) AS silt_achieved,
    n.total_farm_area_silt_is_spread_on,
    'Niti Aayog' AS project
FROM {{ ref('work_order_metric_niti_union') }} AS n

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
    COALESCE(s.silt_target, 0) AS silt_target,
    COALESCE(s.silt_achieved, 0) AS silt_achieved,
    s.total_farm_area_silt_is_spread_on,
    'Project A' AS project
FROM {{ ref('work_order_silt_calc') }} AS s

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
    COALESCE(g.silt_target, 0) AS silt_target,
    COALESCE(g.silt_achieved, 0) AS silt_achieved,
    g.total_farm_area_silt_is_spread_on,
    'GDGS' AS project
FROM {{ ref('work_order_metric_gdgs_union') }} AS g
