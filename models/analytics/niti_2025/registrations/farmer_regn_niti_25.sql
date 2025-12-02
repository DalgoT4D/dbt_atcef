{{ config(
  materialized='table',
  tags=["analytics","analytics_niti_2025", "niti_2025", "niti", "niti_registrations", "registrations_niti_2025"]
) }}

with farmer_data as (
SELECT 
f.*,
l.*,
a.approval_status

FROM 
{{ ref('dim_subjects_farmer_niti_25') }} AS f
LEFT JOIN 
{{ ref('location_niti_25') }} AS l
    ON f.location_id = l.address_id
LEFT JOIN 
{{ ref('approval_status_niti_25') }} AS a
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
stakeholder_responsible,
approval_status
from farmer_data
