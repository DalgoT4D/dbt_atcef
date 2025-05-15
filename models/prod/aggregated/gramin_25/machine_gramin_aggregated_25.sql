{{ config(
  materialized='table',
  tags=["aggregated","aggregated_gramin_niti", "gramin_niti", "gramin_25"]
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
    w.ngo_name,
    m.machine_voided,
    m.machine_approval_status,
    e.machine_sub_id,
    SUM(CAST(e.working_hours_as_per_time AS NUMERIC)) AS total_working_hours,
    MAX(e.date_time) AS date_time
FROM
    {{ ref('encounters_gramin_25') }} AS e
INNER JOIN
    {{ ref('machine_gramin_25') }} AS m
    ON
        e.machine_sub_id = m.machine_id
LEFT JOIN
    {{ ref('work_order_gramin_25') }} AS w
    ON
        e.subject_id = w.work_order_id
WHERE
    e.working_hours_as_per_time IS NOT NULL
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
    e.machine_sub_id,
    w.ngo_name,
    m.machine_approval_status
