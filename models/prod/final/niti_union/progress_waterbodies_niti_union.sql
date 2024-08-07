{{ config(
  materialized='table'
) }}

SELECT 
    dam,
    state,
    village,
    district,
    taluka,
    encounter_type,
    date_time,
    ngo_name,
    project_status
FROM {{ref('progress_waterbodies_niti_22')}}

union all 

SELECT 
    dam,
    state,
    village,
    district,
    taluka,
    encounter_type,
    date_time,
    ngo_name,
    project_status
FROM {{ref('progress_waterbodies_niti_23')}}

union all 

SELECT 
    dam,
    state,
    village,
    district,
    taluka,
    encounter_type,
    date_time,
    ngo_name,
    project_status
FROM {{ref('progress_waterbodies_niti_2024')}}