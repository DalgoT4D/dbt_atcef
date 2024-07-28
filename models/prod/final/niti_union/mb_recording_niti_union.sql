{{ config(
  materialized='table'
) }}


SELECT
    max(date_time) as date_time,
    state, 
    district,
    taluka,
    dam,
    ngo_name,
    village,
    SUM(COALESCE(silt_excavated_as_per_MB_recording, 0)) AS silt_excavated_as_per_MB_recording,
    MAX(COALESCE(silt_target, 0)) AS total_silt_excavated
FROM
   {{ref('work_order_niti_union')}}
group by 
  state,
	district,
	taluka,
	dam,
  ngo_name,
	village