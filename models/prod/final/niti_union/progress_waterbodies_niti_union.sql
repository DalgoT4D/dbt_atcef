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
    project_status
FROM {{ref('progress_waterbodies_niti_23')}}