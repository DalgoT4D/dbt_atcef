{{ config(
  materialized='table'
) }}


SELECT
 "ID" AS eid,
 "Subject_ID" AS subject_id,
 "Subject_type" AS subject_type,
 "Encounter_location" AS encounter_location,
  CAST(TO_DATE("Encounter_date_time", 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS date) AS date_time,
  CAST(observations ->> 'Total Silt carted' AS FLOAT) AS total_silt_carted
  
FROM staging.encounters
