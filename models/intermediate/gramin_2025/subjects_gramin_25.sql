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
        subjects."Location_ID" as address_id,
        subjects."Voided" AS subject_voided,
        subjects.observations ->> 'First name' AS first_name,
        rwb.dam AS dam,
        rwb.district AS district,
        rwb.state AS state,
        rwb.taluka AS taluka,
        rwb.village AS village,
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
        {{ source('source_gramin_25', 'subjects_gramin_25') }} subjects
    LEFT JOIN
        {{ ref('address_gramin_25') }} AS rwb
        ON
            rwb.address_id = subjects."Location_ID"
    WHERE NOT (LOWER(location ->> 'Dam') ~ 'voided')
),


deduplicated AS (
    {{ dbt_utils.deduplicate(
    relation='cte',
    partition_by='uid',
    order_by='uid desc',
   )
}}
)

SELECT *
FROM deduplicated
