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
    e.distance_from_waterbody,
    e.type_of_land_silt_is_spread_on,
    e.total_farm_area_silt_is_spread_on,
    e.date_time,
    e.total_silt_excavated_encounter,
    e.approval_status,
    s.mobile_verified,
    s.first_name,
    s.dam,
    s.district,
    s.state,
    s.taluka,
    s.village,
    s.category_of_farmer,
    s.mobile_number,
    s.ngo_name,
    s.silt_target,
    s.subject_voided
FROM {{ ref('encounter_type') }} e
LEFT JOIN {{ ref('subjects_niti_2025') }} s ON e.subject_id = s.uid
WHERE e.encounter_type = 'Farmer Endline'