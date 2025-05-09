{{ config(
  materialized='table',
  tags=["final","final_niti_union", "niti"]
) }}

SELECT
    dam,
    state,
    village,
    district,
    taluka,
    endline_date,
    farmer_date,
    ngo_name,
    project_status,
    work_order_endline_status
FROM {{ ref('progress_waterbodies_niti_22') }}

UNION ALL

SELECT
    dam,
    state,
    village,
    district,
    taluka,
    endline_date,
    farmer_date,
    ngo_name,
    project_status,
    work_order_endline_status
FROM {{ ref('progress_waterbodies_niti_23') }}

UNION ALL

SELECT
    dam,
    state,
    village,
    district,
    taluka,
    endline_date,
    farmer_date,
    ngo_name,
    project_status,
    work_order_endline_status
FROM {{ ref('progress_waterbodies_niti_2024') }}

UNION ALL

SELECT
    dam,
    state,
    village,
    district,
    taluka,
    endline_date,
    farmer_date,
    ngo_name,
    project_status,
    work_order_endline_status
FROM {{ ref('progress_waterbodies_niti_2025') }}
