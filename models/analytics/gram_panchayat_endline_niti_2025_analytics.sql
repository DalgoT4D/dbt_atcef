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
    e.total_silt_excavated_by_gp_for_non_farm_purpose,
    e.approval_status,
    s.subject_voided,
    s.first_name as work_order_name,
    s.dam,
    s.district,
    s.state,
    s.taluka,
    s.village,
    s.ngo_name,
    s.silt_target
FROM {{ ref('encounter_type') }} e
LEFT JOIN {{ ref('subjects_niti_2025') }} s ON e.subject_id = s.uid
WHERE e.encounter_type = 'Gram Panchayat Endline'