{{ config(
  materialized='table'
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
    COALESCE(n.silt_target, 0) AS silt_target,
    COALESCE(n.silt_achieved, 0) AS silt_achieved,
    'Niti Aayog' AS project
FROM {{ ref('work_order_metric_niti_union') }} n

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
    COALESCE(s.silt_target, 0) AS silt_target,
    COALESCE(s.silt_achieved, 0) AS silt_achieved,
    'Project A' AS project
FROM {{ ref('work_order_silt_calc') }} s

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
    COALESCE(g.silt_target, 0) AS silt_target,
    COALESCE(g.silt_achieved, 0) AS silt_achieved,
    'GDGS' AS project
FROM {{ ref('work_order_metric_gdgs_union') }} g