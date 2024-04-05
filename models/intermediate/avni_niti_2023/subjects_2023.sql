{{ config(
  materialized='table'
) }}

SELECT
    "ID" as uid, 
    observations->>'First name' AS first_name,
    location->>'Dam' AS dam,
    location->>'District' AS district,
    location->>'State' AS state,
    location->>'Taluka' AS taluka,
    location->>'GP/Village' as Village,
    observations ->> 'Type of Machine' as type_of_machine,
    observations ->> 'Category of farmer' as category_of_farmer,
    (observations -> 'Mobile Number'->>'verified')::boolean AS mobile_verified,
    CAST(observations ->> 'Silt to be excavated as per plan' AS FLOAT) AS silt_to_be_excavated
    
FROM {{ source('source_atecf_surveys', 'subjects_2023') }} where location->>'Dam' is not null
