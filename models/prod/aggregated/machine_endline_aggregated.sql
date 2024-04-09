{{ config(
  materialized='table'
) }}


SELECT
    date_time,
    state,
    village,
    district,
    taluka,
    dam,
	type_of_machine,
	working_hours_as_per_time,
	total_working_hours_of_machine_by_time,
	total_working_hours_of_machine
FROM
	{{ ref('work_order_union') }}
WHERE
	encounter_type = 'Excavating Machine Endline' and project_ongoing = 'Ongoing'