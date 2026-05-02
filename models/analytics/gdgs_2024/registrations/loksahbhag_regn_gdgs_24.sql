-- Loksahbhag registration table with cleaned identifiers, location, and approval metadata for gdgs 2024.
{{ config(
  materialized='table',
    tags=["analytics","analytics_gdgs_2024", "registrations_gdgs_2024"]
) }}

SELECT 
  m.subject_id,
  m.registration_date,
  m.subject_type,
  m.location_id,
  m.state_ls,
  m.district_ls,
  m.taluka_ls,
  m.village_ls,
  -- m.ngo_name as stakeholder_responsible,
m.waterbody_name,
m.site_start_date,
m.site_end_date,
m.village_code,
m.silt_excavated_ls::NUMERIC AS silt_excavated_ls,
m.voided

-- Note: Location details are contained within the subjects table itself, so no need to join with a separate location table
-- There is no approval status details. 

FROM 
{{ ref('dim_subjects_loksahbhag_gdgs_24') }} AS m
WHERE m.voided != TRUE
