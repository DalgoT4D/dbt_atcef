{{ config(
  materialized='table'
) }}

SELECT
    "ID" as uid, 
    observations->>'First name' AS first_name,
    observations->>'Last name' AS last_name,
    observations->>'Date of birth' AS date_of_birth,
    observations->>'Gender' AS gender,
    observations->>'Registration_date' AS registration_date,
    observations->>'Registration_location' AS registration_location,
    location->>'Dam' AS dam,
    location->>'District' AS district,
    location->>'State' AS state,
    location->>'Taluka' AS taluka,
    location->>'Village' AS village,
    observations->>'Total silt required' AS total_silt_required,
    observations->>'Silt to be excavated as per plan' AS silt_to_be_excavated_as_per_plan,
    observations->>'Category of farmer' AS category_of_farmer
    
FROM staging.subjects 
