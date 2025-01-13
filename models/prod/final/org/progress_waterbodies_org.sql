{{ config(
  materialized='table'
) }}

SELECT DISTINCT
    n.dam AS dam,
    n.state AS state,
    n.village AS village,
    n.district AS district,
    n.taluka AS taluka,
    CAST(n.endline_date AS DATE) AS endline_date,
    CAST(n.farmer_date AS DATE) AS farmer_date,
    n.ngo_name AS ngo_name,
    n.project_status AS project_status,
    n.work_order_endline_status AS work_order_endline_status,
    NULL AS work_order_id,  
    'Niti Aayog' AS project
FROM {{ ref('progress_waterbodies_niti_union') }} n

UNION

SELECT DISTINCT
    g.dam AS dam,
    g.state AS state,
    g.village AS village,
    g.district AS district,
    g.taluka AS taluka,
    CAST(g.endline_date AS DATE) AS endline_date,
    CAST(g.farmer_date AS DATE) AS farmer_date,
    g.ngo_name AS ngo_name,
    g.project_status AS project_status,
    g.work_order_endline_status AS work_order_endline_status,
    g.work_order_id AS work_order_id,  
    'Project A' AS project
FROM {{ ref('progress_waterbodies_gramin') }} g

UNION

SELECT DISTINCT
    gd.dam AS dam,
    gd.state AS state,
    gd.village AS village,
    gd.district AS district,
    gd.taluka AS taluka,
    CAST(gd.endline_date AS DATE) AS endline_date,
    CAST(gd.farmer_date AS DATE) AS farmer_date,
    gd.ngo_name AS ngo_name,
    gd.project_status AS project_status,
    gd.work_order_endline_status AS work_order_endline_status,
    NULL AS work_order_id, 
    'GDGS' AS project
FROM {{ ref('progress_waterbodies_gdgs_union') }} gd