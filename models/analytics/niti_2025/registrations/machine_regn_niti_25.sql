-- Machine registration table with cleaned identifiers, location, and approval metadata for NITI 2025.
{{ config(
  materialized='table',
  tags=["analytics","analytics_niti_2025", "niti_registrations", "registrations_niti_2025"]
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
{{ ref('dim_subjects_machine_niti_25') }} AS m
LEFT JOIN 
{{ ref('location_niti_25') }} AS l
    ON m.location_id = l.address_id
LEFT JOIN 
{{ ref('approval_status_niti_25') }} AS a
    ON m.subject_id = a.entity_id

WHERE m.voided != TRUE
