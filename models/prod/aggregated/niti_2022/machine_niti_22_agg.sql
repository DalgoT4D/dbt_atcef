{{ config(
  materialized='table',
  tags=["aggregated","aggregated_niti_2022", "niti_2022", "niti"]
) }}


with cte as (SELECT
    subject_id as machine_id,
    first_name as machine_name,
    type_of_machine,
    dam,
    district,
    subject_type,
    state,
    taluka,
    village,
    ngo_name,
    SUM(CAST(total_working_hours_of_machine_by_time AS NUMERIC)) AS total_working_hours,
    MAX(date_time) AS date_time
FROM
    {{ref('work_order_2022')}} 
where encounter_type = 'Excavating Machine Endline'
GROUP BY 
    subject_id,
    first_name,
    type_of_machine,
    dam,
    district,
    subject_type,
    state,
    taluka,
    village,
    ngo_name)

select * from cte where total_working_hours is not null and total_working_hours != 0




