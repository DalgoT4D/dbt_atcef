{{ config(
  materialized='table',
  tags=["intermediate","intermediate_niti_2025", "niti_2025", "niti"]
) }}


WITH mycte AS (
    SELECT
        "ID" AS machine_id,
        "Subject_type" AS subject_type,
        "Voided" AS machine_voided,
        INITCAP(TRIM(COALESCE(observations ->> 'First name'))) AS machine_name,
        observations ->> 'Type of Machine' AS type_of_machine,
        INITCAP(COALESCE(rwb.dam, '')) AS dam,
        INITCAP(COALESCE(rwb.district, '')) AS district,
        CASE
            WHEN
                LOWER(rwb.state) LIKE '%maharashtra%'
                THEN 'Maharashtra'
            WHEN
                LOWER(rwb.state) LIKE '%maharshatra%'
                THEN 'Maharashtra'
            ELSE INITCAP(COALESCE(rwb.state, ''))
        END AS state,
        INITCAP(COALESCE(rwb.state, '')) AS taluka,
        INITCAP(COALESCE(rwb.village, '')) AS village
    FROM
        {{ source('rwb_niti_2025', 'subjects_niti_2025') }}
    LEFT JOIN
        {{ ref('address_niti_2025') }} AS rwb
        ON
           subjects."Location_ID" = rwb.address_id
    WHERE
        "Subject_type" = 'Excavating Machine'
        AND "Voided" = 'False'
),

approval_machines AS (
    SELECT
        d.*,
        a.approval_status AS machine_approval_status
    FROM mycte AS d
    INNER JOIN
        {{ ref('approval_status_niti_2025') }} AS a
        ON d.machine_id = a.entity_id
    WHERE a.entity_type = 'Subject' AND a.approval_status = 'Approved'
)

    {{ dbt_utils.deduplicate(
        relation='approval_machines',
        partition_by='machine_id',
        order_by='machine_id desc'
    ) }}
