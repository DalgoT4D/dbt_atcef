{{ config(
  materialized='table'
) }}


SELECT
 "ID" AS eid,
 "Subject_ID" AS subject_id,
 "Subject_type" AS subject_type,
 "Encounter_location" AS encounter_location,
  "Encounter_type" AS encounter_type,
  observations->>'Working Hours as per time' as working_hours_as_per_time,
  CAST(TO_DATE("Encounter_date_time", 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"') AS date) AS date_time,
  CAST(observations ->> 'Total Silt carted' AS FLOAT) AS total_silt_carted
  
FROM {{ source('source_atecf_surveys', 'encounter_2023') }}
