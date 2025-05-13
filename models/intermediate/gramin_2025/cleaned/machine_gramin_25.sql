{{ config(
  materialized='table',
  tags=["intermediate","intermediate_gramin_niti", "gramin_niti", "gramin_25"]
) }}


WITH machines AS (
    SELECT
        subjects."ID" AS machine_id,
        subjects."Location_ID" AS address_id,
        subjects."Subject_type" AS subject_type,
        subjects."Voided" AS machine_voided,
        INITCAP(TRIM(COALESCE(subjects.observations ->> 'First name'))) AS machine_name,
        subjects.observations ->> 'Type of Machine' AS type_of_machine,
        rwb.dam,
        rwb.district,
        CASE
            WHEN
                LOWER(rwb.state) LIKE '%maharashtra%'
                THEN 'Maharashtra'
            WHEN
                LOWER(rwb.state) LIKE '%maharshatra%'
                THEN 'Maharashtra'
            ELSE INITCAP(COALESCE(rwb.state, ''))
        END AS state,
        INITCAP(COALESCE(rwb.taluka)) AS taluka,
        INITCAP(COALESCE(rwb.village)) AS village
    FROM
        {{ source('source_gramin_25', 'subjects_gramin_25') }} AS subjects
    LEFT JOIN
        {{ ref('address_gramin_25') }} AS rwb
        ON
            subjects."Location_ID" = rwb.address_id
    WHERE
        "Subject_type" = 'Excavating Machine'
),

approval_machines AS (
    SELECT
        d.*,
        a.approval_status AS machine_approval_status
    FROM machines AS d
    INNER JOIN
        {{ ref('approval_status_gramin_25') }} AS a
        ON d.machine_id = a.entity_id
    WHERE a.entity_type = 'Subject' AND a.approval_status = 'Approved'
)

    {{ dbt_utils.deduplicate(
        relation='approval_machines',
        partition_by='machine_id',
        order_by='machine_id desc'
    ) }}
