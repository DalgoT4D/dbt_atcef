{{ config(
  materialized='table'
) }}

select 
    "ID" AS eid,
    "Subject_ID" AS subject_id,
    "Subject_type" AS subject_type,
    "Encounter_location" AS encounter_location,
    observations ->> 'Total Silt carted' as total_silt_carted,
    observations ->> 'Total silt excavated' as total_silt_excavated, 
    observations ->> 'Silt excavated as per MB recording' as silt_excavated_as_per_MB_recording
FROM niti_2022.encounters