{{ config(
  materialized='table'
) }}


SELECT
	uid,
	date_time,
	first_name AS work_order,
	dam as waterbodies,
	state, 
	district,
	taluka,
	village,
	total_working_hours_of_machine,
	type_of_machine
	
FROM
	{{ref('work_order_2024')}}
WHERE
	encounter_type = 'Excavating Machine Endline'
	