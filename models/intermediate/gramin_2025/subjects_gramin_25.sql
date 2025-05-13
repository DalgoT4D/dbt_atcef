{{ config(
  materialized='table',
  tags=["intermediate","intermediate_gramin", "gramin_niti", "gramin_25"]
) }}

WITH cte AS (
    SELECT
        subjects."ID" AS uid,
        (
            subjects.observations -> 'Mobile Number' ->> 'verified'
        )::boolean AS mobile_verified,
        subjects."Location_ID" AS address_id,
        subjects."Voided" AS subject_voided,
        subjects.observations ->> 'First name' AS first_name,
        rwb.dam,
        rwb.district,
        rwb.state,
        rwb.taluka,
        rwb.village,
        subjects.observations ->> 'Type of Machine' AS type_of_machine,
        subjects.observations ->> 'Category of farmer' AS category_of_farmer,
        subjects.observations -> 'Mobile Number' ->> 'phoneNumber' AS mobile_number,
        COALESCE(
            NULLIF(rwb.stakeholder_responsible, ''), 'Unknown'
        ) AS ngo_name,
        ROUND(((COALESCE(
            NULLIF(TRIM(rwb.silt_target::text), ''),
            '0'
        ))::float)::numeric, 2) AS silt_target
    FROM
        {{ source('source_gramin_25', 'subjects_gramin_25') }} AS subjects
    LEFT JOIN
        {{ ref('address_gramin_25') }} AS rwb
        ON
            subjects."Location_ID" = rwb.address_id
    WHERE NOT (LOWER(location ->> 'Dam') ~ 'voided')
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
    INNER JOIN {{ ref('approval_status_gramin_25') }} AS a
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
