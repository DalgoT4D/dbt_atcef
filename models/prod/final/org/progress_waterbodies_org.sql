{{ config(
  materialized='table',
  tags=["final","final_org"]
) }}

SELECT DISTINCT
    n.dam,
    n.state,
    n.village,
    n.district,
    n.taluka,
    CAST(n.endline_date AS DATE) AS endline_date,
    CAST(n.farmer_date AS DATE) AS farmer_date,
    n.ngo_name,
    n.project_status,
    n.work_order_endline_status,
    NULL AS work_order_id,
    'Niti Aayog' AS project
FROM {{ ref('progress_waterbodies_niti_union') }} AS n

UNION

SELECT DISTINCT
    g.dam,
    g.state,
    g.village,
    g.district,
    g.taluka,
    CAST(g.endline_date AS DATE) AS endline_date,
    CAST(g.farmer_date AS DATE) AS farmer_date,
    g.ngo_name,
    g.project_status,
    g.work_order_endline_status,
    g.work_order_id,
    'Project A' AS project
FROM {{ ref('progress_waterbodies_gramin') }} AS g

UNION

SELECT DISTINCT
    gd.dam,
    gd.state,
    gd.village,
    gd.district,
    gd.taluka,
    CAST(gd.endline_date AS DATE) AS endline_date,
    CAST(gd.farmer_date AS DATE) AS farmer_date,
    gd.ngo_name,
    gd.project_status,
    gd.work_order_endline_status,
    NULL AS work_order_id,
    'GDGS' AS project
FROM {{ ref('progress_waterbodies_gdgs_union') }} AS gd
