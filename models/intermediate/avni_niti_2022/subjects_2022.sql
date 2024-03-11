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
    CAST(observations ->> 'Silt to be excavated as per plan' AS FLOAT) AS silt_to_be_excavated
    
FROM niti_2022.subjects 