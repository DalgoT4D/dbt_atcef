{{ config(
  materialized='table'
) }}


SELECT
    m.machine_id,
    m.machine_name,
    m.type_of_machine,
    m.dam,
    m.district,
    m.subject_type,
    m.state,
    m.taluka,
    m.village,
    m.machine_voided,
    m.machine_approval_status,
    e.machine_sub_id,
    SUM(CAST(e.working_hours_as_per_time AS NUMERIC)) AS total_working_hours,
    MAX(e.date_time) AS date_time
FROM
    {{ref('encounter_2022')}} e
LEFT JOIN
    {{ref('machine_niti_22')}} m
ON
    e.machine_sub_id = m.machine_id
GROUP BY
    m.machine_id,
    m.machine_name,
    m.type_of_machine,
    m.dam,
    m.district,
    m.subject_type,
    m.state,
    m.taluka,
    m.village,
    m.machine_voided,
    m.machine_approval_status,
    e.machine_sub_id


