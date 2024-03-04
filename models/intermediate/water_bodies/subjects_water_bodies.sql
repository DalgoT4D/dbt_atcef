SELECT
    "ID", 
    location->>'Dam' AS dam,
    location->>'Dam External ID' AS dam_external_id,
    location->>'District' AS district,
    location->>'District External ID' AS district_external_id,
    location->>'State' AS state,
    location->>'State External ID' AS state_external_id,
    location->>'Taluka' AS taluka,
    location->>'Taluka External ID' AS taluka_external_id,
    location->>'Village' AS village,
    location->>'Village External ID' AS village_external_id,
    observations->>'Aadhar number' AS aadhar_number,
    observations->>'Date of birth' AS date_of_birth,
    observations->>'Distance from farm' AS distance_from_farm,
    observations->>'First name' AS first_name,
    observations->>'Gender' AS gender,
    observations->>'INR 500 collected during registration' AS inr_500_collected_during_registration,
    observations->>'Landholding' AS landholding,
    observations->>'Last name' AS last_name,
    observations->>'Mobile number old' AS mobile_number_old,
    observations->>'Number of dependent family members' AS number_of_dependent_family_members
    
FROM rejuvenating_water_bodies.subjects 
