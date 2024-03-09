{{ config(
  materialized='table'
) }}


SELECT
 "ID" AS eid,
 "Subject_ID" AS subject_id,
 "Subject_type" AS subject_type,
 "Encounter_location" AS encounter_location,
  observations ->> 'Total silt excavated' AS total_silt_excavated,
  observations ->> 'Total Silt carted' AS total_silt_carted,
  observations ->> 'Silt excavated as per MB recording' AS silt_excavated_as_per_MB_recording
  
FROM staging.encounters
