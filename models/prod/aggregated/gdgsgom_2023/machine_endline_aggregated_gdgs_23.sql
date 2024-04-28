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
	ROUND(sum(total_working_hours_of_machine), 2) as total_working_hours_of_machine,
	type_of_machine
	
FROM
	{{ref('work_order_gdgs_2023')}}
WHERE
	encounter_type = 'Excavating Machine Endline' and
    total_working_hours_of_machine is not NULL and 
    total_working_hours_of_machine != 0
group by 
	dam,
	state, 
	district,
	taluka,
	village,
	type_of_machine