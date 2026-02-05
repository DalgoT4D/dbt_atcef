-- Raw encounter listing that keeps subject identifiers, encounter types, timestamps, and the JSON observations.
{{ config(
  materialized='table',
  tags=["analytics", "gdgs_2025", "gdgs", "source", "source_cleaned_gdgs_2025"]
) }}


SELECT 
"ID" as eid,
"Subject_ID" as subject_id,
"Subject_type" as subject_type,
"Encounter_type" as encounter_type,
"Encounter_date_time" as encounter_date_time,
observations,
"Voided" as voided

FROM {{ source('gdgs_25_surveys', 'encounters_gdgs_2025') }}