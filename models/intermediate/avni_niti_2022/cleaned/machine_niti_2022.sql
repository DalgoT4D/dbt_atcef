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
        {{ source('source_atecf_surveyss', 'subjects_2022') }}
    WHERE
        "Voided" = 'False'
),
approval_machines AS (
    SELECT d.*, a.approval_status AS machine_approval_status
    FROM mycte d
    JOIN {{ ref('approval_statuses_niti_2022') }} a ON d.machine_id = a.entity_id
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
    e.total_working_hours_of_machine AS total_working_hours,
    e.date_time AS latest_date_time
FROM
    {{ref('encounter_2022')}} e
JOIN
    deduplicated_machines m
ON
    e.subject_id = m.machine_id
Where total_working_hours_of_machine is not null
