{{ config(
  materialized='table'
) }}

SELECT
    "ID" as uid, -- Replace 'id' with the actual unique identifier column name if different
    observations->>'First name' AS first_name,
    location->>'Dam' as dam,
    location->>'District' as district,
    location->>'State' as state,
    location->>'Taluka' as Taluka,
    location->>'GP/Village' as Village,
    CAST(observations ->> 'Silt to be excavated as per plan' AS FLOAT) AS silt_to_be_excavated
    
FROM {{ source('source_atecf_surveyss', 'subjects_2022') }}