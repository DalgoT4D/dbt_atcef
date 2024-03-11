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
    location->>'Village' AS village,
    CAST(observations ->> 'Silt to be excavated as per plan' AS FLOAT) AS silt_to_be_excavated
    
FROM staging.subjects 
