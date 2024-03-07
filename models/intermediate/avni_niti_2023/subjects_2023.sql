{{ config(
  materialized='table'
) }}

SELECT
    "ID" as uid, 
    observations->>'First name' AS first_name,
    observations->>'Last name' AS last_name,
    observations->>'Date of birth' AS date_of_birth,
    observations->>'Gender' AS gender,
    location->>'Dam' AS dam,
    location->>'District' AS district,
    location->>'State' AS state,
    location->>'Taluka' AS taluka,
    location->>'Village' AS village,
    observations->>'Village of farmer' AS village_of_farmer,
    observations->>'Total silt required' AS total_silt_required,
    observations->>'Silt to be excavated as per plan' AS silt_to_be_excavated_as_per_plan,
    observations->>'Wells nearby' AS wells_nearby,
    observations->>'Distance from farm' AS distance_from_farm,
    observations->>'INR 500 collected during registration' AS inr_500_collected_during_registration,
    observations->>'Reason for not collecting registration fee' AS reason_for_not_collecting_registration_fee,
    observations->>'Landholding' AS landholding,
    observations->>'Number of dependent family members' AS number_of_dependent_family_members
    
FROM staging.subjects 
