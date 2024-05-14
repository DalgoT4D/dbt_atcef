{{ config(
  materialized='table'
) }}

WITH cte AS (
    SELECT
        "ID" AS uid, 
        observations->>'First name' AS first_name,
        location->>'Dam' AS dam,
        location->>'District' AS district,
        location->>'State' AS state,
        location->>'Taluka' AS taluka,
        location->>'GP/Village' AS village,
        observations->>'Type of Machine' AS type_of_machine,
        observations->>'Category of farmer' AS category_of_farmer,
        observations->'Mobile Number'->>'phoneNumber' AS mobile_number,
        (observations->'Mobile Number'->>'verified')::boolean AS mobile_verified,
        COALESCE(NULLIF(rwb."Stakeholder responsible", ''), 'Unknown') AS ngo_name,
        ROUND(CAST(CAST(
            COALESCE(
                NULLIF(TRIM(rwb."Estimated quantity of Silt"::text), ''),
                '0'
            ) AS FLOAT
        ) AS numeric), 2) AS silt_target,
        "Voided" AS voided
    FROM 
        {{ source('source_atecf_surveys', 'subjects_2023') }}
    RIGHT JOIN 
        rwb_niti_2023."rwb2023_address" AS rwb 
    ON 
        location->>'Dam' = rwb."Dam" 
    WHERE NOT (LOWER(location->>'Dam') ~ 'voided')
),

removing_nulls AS (
    SELECT * 
    FROM cte 
    WHERE dam IS NOT NULL 
        AND district IS NOT NULL
        AND taluka IS NOT NULL
        AND state IS NOT NULL
        AND village IS NOT NULL
        AND NOT voided
),

approved_subjects AS (
    SELECT r.*
    FROM removing_nulls r
    JOIN {{ ref('approval_statuses_niti_2023') }} a 
    ON r.uid = a.entity_id
    WHERE a.entity_type = 'Subject' AND a.approval_status = 'Approved'
),

deduplicated AS (
    {{ dbt_utils.deduplicate(
    relation='removing_nulls',
    partition_by='uid',
    order_by='uid desc',
   )
}}
)

SELECT *
FROM deduplicated
