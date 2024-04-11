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
    CAST(observations ->> 'Silt to be excavated as per plan' AS FLOAT) AS silt_to_be_excavated,
    rwb."Stakeholder responsible" AS ngo_name
FROM 
    {{ source('source_atecf_surveys', 'subjects_2023') }}
RIGHT JOIN 
    rwb_niti_2023."Niti 2023 Address" AS rwb 
ON 
    location->>'Dam' = rwb."Dam" )


select * from cte where dam IS NOT NULL 
      and district is not null
      and taluka is not null
      and state is not null
      and village is not null

