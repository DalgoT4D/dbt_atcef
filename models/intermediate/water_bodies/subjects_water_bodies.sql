{{ config(
  materialized='table'
) }}

SELECT
    "ID", 
    observations->>'First name' AS first_name,
    observations->>'Last name' AS last_name,
    observations->>'Date of birth' AS date_of_birth,
    observations->>'Gender' AS gender,
    location->>'Dam' AS dam,
    location->>'District' AS district,
    location->>'State' AS state,
    location->>'Taluka' AS taluka,
    location->>'Village' AS village,
    observations->>'Distance from farm' AS distance_from_farm,
    observations->>'INR 500 collected during registration' AS inr_500_collected_during_registration,
    observations->>'Landholding' AS landholding,
    observations->>'Number of dependent family members' AS number_of_dependent_family_members
    
FROM rejuvenating_water_bodies.subjects 
