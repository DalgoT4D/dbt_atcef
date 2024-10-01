{{ config(
  materialized='table'
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
FROM {{ref('progress_waterbodies_niti_22')}}

union all 

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
FROM {{ref('progress_waterbodies_niti_23')}}

union all 

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
FROM {{ref('progress_waterbodies_niti_2024')}}