{{ config(
  materialized='table',
  tags=["intermediate","intermediate_gdgs_2023", "gdgs_2023", "gdgs"]
) }}


WITH mycte AS (
    SELECT
        "ID" AS machine_id,
        "Subject_type" AS subject_type,
        "Voided" AS machine_voided,
        INITCAP(TRIM(COALESCE(observations ->> 'First name'))) AS machine_name,
        observations ->> 'Type of Machine' AS type_of_machine,
        INITCAP(COALESCE(location ->> 'Dam')) AS dam,
        INITCAP(COALESCE(location ->> 'District')) AS district,
        CASE
            WHEN
                LOWER(location ->> 'State') LIKE '%maharashtra%'
                THEN 'Maharashtra'
            WHEN
                LOWER(location ->> 'State') LIKE '%maharshatra%'
                THEN 'Maharashtra'
            ELSE INITCAP(COALESCE(location ->> 'State', ''))
        END AS state,
        INITCAP(COALESCE(location ->> 'Taluka')) AS taluka,
        INITCAP(COALESCE(location ->> 'GP/Village')) AS village
    FROM
        {{ source('source_gdgsom_surveys_2023', 'subjects_2023') }}
    WHERE
        "Subject_type" = 'Excavating Machine' AND "Voided" = 'False'
),

approval_machines AS (
    SELECT
        d.*,
        a.approval_status AS machine_approval_status
    FROM mycte AS d
    INNER JOIN
        {{ ref('approval_statuses_gdgs_2023') }} AS a
        ON d.machine_id = a.entity_id
    WHERE a.entity_type = 'Subject' AND a.approval_status = 'Approved'
)

    {{ dbt_utils.deduplicate(
        relation='approval_machines',
        partition_by='machine_id',
        order_by='machine_id desc'
    ) }}
