{{ config(
  materialized='table'
) }}


WITH mycte AS (
    SELECT
        "ID" AS machine_id,
        INITCAP(TRIM(COALESCE(observations->>'First name'))) AS machine_name,
        observations->>'Type of Machine' AS type_of_machine,
        INITCAP(COALESCE(location->>'Dam')) AS dam,
        INITCAP(COALESCE(location->>'District')) AS district,
        "Subject_type" AS subject_type,
        CASE
            WHEN LOWER(location->>'State') LIKE '%maharashtra%' THEN 'Maharashtra'
            WHEN LOWER(location->>'State') LIKE '%maharshatra%' THEN 'Maharashtra'
            ELSE INITCAP(COALESCE(location->>'State', ''))
        END AS state,
        INITCAP(COALESCE(location->>'Taluka')) AS taluka,
        INITCAP(COALESCE(location->>'GP/Village')) AS village,
        "Voided" AS machine_voided
    FROM
        {{ source('source_atecf_surveys', 'subjects_2023') }}
    WHERE
        "Subject_type" = 'Excavating Machine' AND "Voided" = 'False' and NOT (LOWER(location->>'Dam') ~ 'voided') 
),
approval_machines AS (
    SELECT d.*, a.approval_status AS machine_approval_status
    FROM mycte d
    JOIN {{ ref('approval_statuses_niti_2023') }} a ON d.machine_id = a.entity_id
    WHERE a.entity_type = 'Subject' AND a.approval_status = 'Approved'
),
deduplicated_machines AS (
    {{ dbt_utils.deduplicate(
        relation='approval_machines',
        partition_by='machine_id',
        order_by='machine_id desc'
    ) }}
)


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
    SUM(e.working_hours_as_per_time) AS total_working_hours,
    MAX(e.date_time) AS latest_date_time
FROM
    {{ref('encounter_2023')}} e
JOIN
    deduplicated_machines m
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
