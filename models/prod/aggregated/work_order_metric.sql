{{ config(
  materialized='table'
) }}

SELECT
	MAX(date_time) AS date_time,
	ngo_name,
	state,
	district,
	taluka,
	dam,
	village,
	COALESCE(MAX(silt_target), 0) AS silt_target,
	COALESCE(CAST(ROUND(SUM(CASE WHEN total_silt_carted::text <> 'NaN' THEN total_silt_carted ELSE 0 END)) AS numeric), 0)  + 
    COALESCE(MAX(CAST(total_silt_excavated_by_GP_for_non_farm_purpose AS numeric)), 0) AS silt_achieved
FROM
	{{ref ('work_order_union')}}
WHERE
	encounter_type = 'Work order daily Recording - Farmer'
	AND project_ongoing = 'Ongoing'
	AND silt_target != 0
GROUP BY
	state,
	district,
	taluka,
	dam,
	village,
	ngo_name