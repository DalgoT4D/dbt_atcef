{{ config(
  materialized='table'
) }}


SELECT distinct dam, max(date_time) as date, district, state, taluka, village, max(silt_to_be_excavated) as silt_target, sum(total_silt_carted) as silt_to_be_excavated
FROM
	{{ ref('work_order_union') }} 
WHERE
	encounter_type = 'Work order daily Recording - Farmer'
Group by
dam, district, state, taluka, village
