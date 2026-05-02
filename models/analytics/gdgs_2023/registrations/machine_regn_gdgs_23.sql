-- Machine registration table with cleaned identifiers, location, and approval metadata for gdgs 2023.
{{ config(
  materialized='table',
    tags=["analytics","analytics_gdgs_2023", "registrations_gdgs_2023"]
) }}

SELECT 
  m.subject_id,
  m.registration_date,
  m.subject_type,
  m.location_id,
  -- m.ngo_name as stakeholder_responsible,
  UPPER(REPLACE(REPLACE(REGEXP_REPLACE(
                COALESCE(m.machine_name::TEXT, ''),  -- 1. Cast to TEXT for string operations
                '[-\s]+', -- 2. Target ONE OR MORE hyphens (-) or whitespace characters (\s)
                '',
                'g'
            ),CHR(9), 
            ''),CHR(160),''))::VARCHAR AS machine_name,
  m.machine_type,
  m.contractor_name,
  m.contractor_mobile_number,
  m.voided,
  l.*,
  a.approval_status,
  a.status_date_time as approval_date_time


FROM 
{{ ref('dim_subjects_machine_gdgs_23') }} AS m
LEFT JOIN 
{{ ref('location_gdgs_23') }} AS l
    ON m.location_id = l.address_id
LEFT JOIN 
{{ ref('approval_status_gdgs_23') }} AS a
    ON m.subject_id = a.entity_id

WHERE m.voided != TRUE
AND l.state IS NOT NULL -- some machines do not have coresponding location, because there is no silt target attached to those locations