{{ config(
  materialized='table',
  tags=["intermediate","intermediate_gdgs_2024", "gdgs_2024", "gdgs"]
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
        {{ source('source_gdgsom_surveys', 'subjects_2024') }}
    WHERE
        "Subject_type" = 'Excavating Machine' AND "Voided" = 'False'
),
approval_machines AS (
    SELECT d.*, a.approval_status AS machine_approval_status
    FROM mycte d
    JOIN {{ ref('approval_statuses_gdgs_24') }} a ON d.machine_id = a.entity_id
    WHERE a.entity_type = 'Subject' AND a.approval_status = 'Approved'
)


    {{ dbt_utils.deduplicate(
        relation='approval_machines',
        partition_by='machine_id',
        order_by='machine_id desc'
    ) }}
