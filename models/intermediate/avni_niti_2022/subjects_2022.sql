{{ config(
  materialized='table',
  tags=["intermediate","intermediate_niti_2022"]
) }}

with cte as (SELECT
    "ID" AS uid, -- Replace 'id' with the actual unique identifier column name if different
    observations->>'First name' AS first_name,
    location->>'Dam' AS dam,
    location->>'District' AS district,
    location->>'State' AS state,
    location->>'Taluka' AS taluka,
    location->>'GP/Village' AS village,
    NULL AS category_of_farmer,
    observations ->> 'Type of Machine' AS type_of_machine,
    observations ->'Mobile Number'->>'phoneNumber' AS mobile_number,
    (observations ->'Mobile Number'->>'verified')::boolean AS mobile_verified,
    COALESCE(NULLIF(rwb."Project/NGO", ''), 'Unknown') AS ngo_name,
    CAST(COALESCE(NULLIF(rwb."Estimated quantity of Silt", ''), '0') AS numeric) AS silt_target,
    "Voided" as subject_voided

FROM {{ source('source_atecf_surveyss', 'subjects_2022') }} 
RIGHT JOIN rwb_niti_2022.rwbniti22 AS rwb ON location->>'Dam' = rwb."Dam"
WHERE NOT (LOWER(location->>'Dam') ~ 'voided')
),

removing_nulls as (select * from cte where dam IS NOT NULL
      AND district is not null
      AND taluka is not null
      AND state is not null
      AND village is not null 
      AND subject_voided = 'false'
      ),

approved_subjects AS (
    SELECT r.*
    FROM removing_nulls r
    JOIN {{ ref('approval_statuses_niti_2022') }} a 
    ON r.uid = a.entity_id
    WHERE a.entity_type = 'Subject' AND a.approval_status = 'Approved'
),

deduplicated AS ({{ dbt_utils.deduplicate(
    relation='approved_subjects',
    partition_by='uid',
    order_by='uid desc',
   )
}})

SELECT *
FROM deduplicated

