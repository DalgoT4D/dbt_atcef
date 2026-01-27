-- Raw encounter listing that keeps subject identifiers, encounter types, timestamps, and the JSON observations.
{{ config(
  materialized='table',
  tags=["analytics", "gramin_2025", "gramin", "source", "source_cleaned_gramin_2025"]
) }}


SELECT 
"ID" as eid,
"Subject_ID" as subject_id,
"Subject_type" as subject_type,
"Encounter_type" as encounter_type,
"Encounter_date_time" as encounter_date_time,
observations,
"Voided" as voided

FROM {{ source('source_gramin', 'encounters_gramin') }}