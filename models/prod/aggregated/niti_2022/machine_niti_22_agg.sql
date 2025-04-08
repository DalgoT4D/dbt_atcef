{{ config(
  materialized='table',
  tags=["aggregated","aggregated_niti_2022", "niti_2022", "niti"]
) }}


with cte as (
    select
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
        SUM(
            CAST(total_working_hours_of_machine_by_time as NUMERIC)
        ) as total_working_hours,
        MAX(date_time) as date_time
    from
        {{ ref('work_order_2022') }}
    where encounter_type = 'Excavating Machine Endline'
    group by
        subject_id,
        first_name,
        type_of_machine,
        dam,
        district,
        subject_type,
        state,
        taluka,
        village,
        ngo_name
)

select * from cte
where total_working_hours is not null and total_working_hours != 0
