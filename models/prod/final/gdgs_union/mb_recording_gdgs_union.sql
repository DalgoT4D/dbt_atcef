{{ config(
  materialized='table',
  tags=["final","final_gdgs_union", "gdgs"]
) }}


with cte as (SELECT
    max(date_time) as date_time,
    state, 
    district,
    taluka,
    dam,
    ngo_name,
    village,
    SUM(COALESCE(silt_excavated_as_per_MB_recording, 0)) AS silt_excavated_as_per_MB_recording,
    MAX(COALESCE(silt_to_be_excavated_as_per_plan, 0)) AS total_silt_excavated
FROM
    {{ref('work_order_gdgs_union')}}
group by 
    state,
	district,
	taluka,
	dam,
    ngo_name,
	village)

select * from cte where total_silt_excavated != 0
