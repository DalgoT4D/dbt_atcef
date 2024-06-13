{{ config(
  materialized='table'
) }}


select latest_date_time, 
       machine_name, 
	   type_of_machine,
	   state,
	   district,
	   taluka,
	   village, 
	   dam,
	   machine_voided, 
	   machine_approval_status,
	   total_working_hours
FROM
    {{ ref('machine_niti_2022') }} 
UNION ALL 
select 
       latest_date_time, 
       machine_name, 
	   type_of_machine,
	   state,
	   district,
	   taluka,
	   village, 
	   dam,
	   machine_voided, 
	   machine_approval_status,
	   total_working_hours 
FROM
    {{ ref('machine_niti_2023') }} 