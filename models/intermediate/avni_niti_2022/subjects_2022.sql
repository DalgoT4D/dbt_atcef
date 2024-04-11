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
    CAST(observations ->> 'Silt to be excavated as per plan' AS FLOAT) AS silt_to_be_excavated,
    rwb."Project/NGO" AS ngo_name
FROM {{ source('source_atecf_surveyss', 'subjects_2022') }} -- Assuming this is a correct reference to a source in dbt
RIGHT JOIN rwb_niti_2022."RWB Niti 2022" AS rwb ON location->>'Dam' = rwb."Dam")

select * from cte where dam IS NOT NULL
      and district is not null
      and taluka is not null
      and state is not null
      and village is not null

