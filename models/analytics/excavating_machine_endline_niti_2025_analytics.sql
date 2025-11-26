{{ config(
  materialized='table',
  tags=["analytics", "niti_2025", "niti"]
) }}

SELECT 
    e.eid,
    e.subject_id,
    e.subject_type,
    e.encounter_location,
    e.encounter_type,
    e.voided,
    e.date_time,
    e.total_working_hours_of_machine,
    e.approval_status,
    s.type_of_machine,
    s.dam,
    s.district,
    s.state,
    s.taluka,
    s.village,
    s.subject_voided,
    s.first_name as machine_name,
    s.ngo_name,
    s.silt_target
FROM {{ ref('encounter_type') }} e
LEFT JOIN {{ ref('subjects_niti_2025') }} s ON e.subject_id = s.uid
WHERE e.encounter_type = 'Excavating Machine Endline'