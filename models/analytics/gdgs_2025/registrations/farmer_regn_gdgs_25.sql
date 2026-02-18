-- Farmer registration mart combining subject, location, and approval details for gdgs 2025.
{{ config(
  materialized='table',
  tags=["analytics","analytics_gdgs_2025", "registrations_gdgs_2025"]
) }}

with farmer_data as (
SELECT 
f.*,
l.*,
a.approval_status

FROM 
{{ ref('dim_subjects_farmer_gdgs_25') }} AS f
LEFT JOIN 
{{ ref('location_gdgs_25') }} AS l
    ON f.location_id = l.address_id
LEFT JOIN 
{{ ref('approval_status_gdgs_25') }} AS a
    ON f.subject_id = a.entity_id

WHERE f.voided != TRUE)

select 
subject_id,
registration_date,
farmer_first_name as farmer_name,
land_holding_acres as land_holding,
mobile_number,
mobile_verified_status,
farmer_category,
total_silt_required,
number_hywas_required,
number_trolleys_required,
capacity_trolleys_cum,
farmer_contribution_per_trolley,
silt_target,
state,
district,
taluka,
village,
dam,
gram_panchayat_name as gp,
stakeholder_responsible,
approval_status
from farmer_data
WHERE state IS NOT NULL -- some farmers do not have coresponding location, because there is no silt target attached to those locations