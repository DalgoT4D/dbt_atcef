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
FROM {{ref('progress_waterbodies_gdgs_23')}}

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
FROM {{ref('progress_waterbodies_gdgs_24')}}