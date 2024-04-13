{{ config(
  materialized='table'
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
    rwb."Project/NGO" AS ngo_name,
    CAST(COALESCE(NULLIF(rwb."Estimated quantity of Silt", ''), '0') AS numeric) AS silt_target

FROM {{ source('source_atecf_surveyss', 'subjects_2022') }} -- Assuming this is a correct reference to a source in dbt
RIGHT JOIN rwb_niti_2022.rwbniti22 AS rwb ON location->>'Dam' = rwb."Dam")

select * from cte where dam IS NOT NULL
      and district is not null
      and taluka is not null
      and state is not null
      and village is not null

