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
    e.machine_sub_id,
    e.working_hours_as_per_time,
    e.date_time,
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
LEFT JOIN {{ ref('subjects_niti_2025') }} s ON e.machine_sub_id = s.uid
WHERE e.encounter_type = 'Work order daily Recording - Machine'