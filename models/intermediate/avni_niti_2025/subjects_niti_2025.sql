{{ config(
  materialized='table',
  tags=["intermediate","intermediate_niti_2025", "niti_2025", "niti"]
) }}

WITH cte AS (
    SELECT
        "ID" AS uid,
        (
            observations -> 'Mobile Number' ->> 'verified'
        )::boolean AS mobile_verified,
        "Voided" AS subject_voided,
        observations ->> 'First name' AS first_name,
        rwb.dam,
        rwb.district,
        rwb.state,
        rwb.taluka,
        rwb.village,
        observations ->> 'Type of Machine' AS type_of_machine,
        observations ->> 'Category of farmer' AS category_of_farmer,
        observations -> 'Mobile Number' ->> 'phoneNumber' AS mobile_number,
        COALESCE(
            NULLIF(rwb.stakeholder_responsible, ''), 'Unknown'
        ) AS ngo_name,
        ROUND(((COALESCE(
            NULLIF(TRIM(rwb.silt_target::text), ''),
            '0'
        ))::float)::numeric, 2) AS silt_target
    FROM
        {{ source('rwb_niti_2025', 'subjects_niti_2025') }}
    LEFT JOIN
        {{ ref('address_niti_2025') }} AS rwb
        ON
            location ->> 'Nalla' = rwb.dam
    WHERE NOT (LOWER(location ->> 'Nalla') ~ 'voided')
),

removing_nulls AS (
    SELECT *
    FROM cte
    WHERE
        dam IS NOT NULL
        AND district IS NOT NULL
        AND taluka IS NOT NULL
        AND state IS NOT NULL
        AND village IS NOT NULL
        AND subject_voided = 'false'
),

approved_subjects AS (
    SELECT r.*
    FROM removing_nulls AS r
    INNER JOIN {{ ref('approval_status_niti_2025') }} AS a
        ON r.uid = a.entity_id
    WHERE a.entity_type = 'Subject' AND a.approval_status = 'Approved'
),

deduplicated AS (
    {{ dbt_utils.deduplicate(
    relation='approved_subjects',
    partition_by='uid',
    order_by='uid desc',
   )
}}
)

SELECT *
FROM deduplicated
