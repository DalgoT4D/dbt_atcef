{{ config(
  materialized='table'
) }}

SELECT 
    dam,
    ngo_name,
    state,
    village,
    district,
    taluka,
    endline_date,
    farmer_date,
    project_status,
    work_order_endline_status
FROM {{ref('progress_waterbodies_gdgs_23')}}

union all 

SELECT 
    dam,
    ngo_name,
    state,
    village,
    district,
    taluka,
    endline_date,
    farmer_date,
    project_status,
    work_order_endline_status
FROM {{ref('progress_waterbodies_gdgs_24')}}