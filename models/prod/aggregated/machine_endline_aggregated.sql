{{ config(
  materialized='table'
) }}

SELECT
	max(date_time) as date_time,
	ngo_name,
	state,
	village,
	district,
	taluka,
	dam,
	type_of_machine,
	ROUND(sum(total_working_hours_of_machine), 2) as total_working_hours_of_machine
FROM
	prod_aggregated.work_order_union
WHERE
	encounter_type = 'Excavating Machine Endline'
	AND total_working_hours_of_machine IS NOT NULL
GROUP BY
    ngo_name,
	dam,
	state,
	district,
	taluka,
	village,
	type_of_machine