{{ config(
  materialized='table'
) }}

with cte as (SELECT
    "ID" AS uid, 
    observations->>'First name' AS first_name,
    location->>'Dam' AS dam,
    location->>'District' AS district,
    location->>'State' AS state,
    location->>'Taluka' AS taluka,
    location->>'GP/Village' AS village,
    observations ->> 'Type of Machine' AS type_of_machine,
    observations ->> 'Category of farmer' AS category_of_farmer,
    observations ->'Mobile Number'->>'phoneNumber' AS mobile_number,
    (observations -> 'Mobile Number'->>'verified')::boolean AS mobile_verified,
    COALESCE(NULLIF(rwb."Stakeholder responsible", ''), 'Unknown') AS ngo_name,
    ROUND(CAST(CAST(
        COALESCE(
            NULLIF(TRIM(rwb."Estimated quantity of Silt"::text), ''),
            '0'
        ) AS FLOAT
    ) as numeric), 2) AS silt_target,
    "Voided" as Voided
FROM 
    {{ source('source_atecf_surveys', 'subjects_2023') }}

RIGHT JOIN 
    rwb_niti_2023."rwb2023_address" AS rwb 
ON 
    location->>'Dam' = rwb."Dam" 
WHERE NOT (LOWER(location->>'Dam') ~ 'voided')),


removing_nulls as (select * from cte where dam IS NOT NULL 
      AND district is not null
      AND taluka is not null
      AND state is not null
      AND village is not null
      AND Voided is FALSE)


{{ dbt_utils.deduplicate(
    relation='removing_nulls',
    partition_by='uid',
    order_by='uid desc',
   )
}}


