{{ config(
  materialized='table'
) }}


SELECT
	max(date_time) as date_time,
	dam as waterbodies,
	state, 
	district,
	taluka,
	village,
	sum(total_working_hours_of_machine) as total_working_hours_of_machine,
	type_of_machine
	
FROM
	{{ref('work_order_2024')}}
WHERE
	encounter_type = 'Excavating Machine Endline'
group by 
	dam,
	state, 
	district,
	taluka,
	village,
	type_of_machine