{{ config(
  materialized='table'
) }}

SELECT DISTINCT ON (mobile_number)
	mobile_number,
	mobile_verified,
	encounter_type,
	state,
	district,
	taluka,
	village,
	dam
FROM
	{{ ref('work_order_union') }} 
WHERE
	project_ongoing = 'Ongoing'
	AND encounter_type = 'Farmer Endline'
