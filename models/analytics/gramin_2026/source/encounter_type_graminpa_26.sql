-- Raw encounter listing that keeps subject identifiers, encounter types, timestamps, and the JSON observations.
{{ config(
  materialized='table',
  tags=["analytics", "analytics_gramin_2026", "source", "source_cleaned_gramin_2026"]
) }}


SELECT 
"ID" as eid,
"Subject_ID" as subject_id,
"Subject_type" as subject_type,
"Encounter_type" as encounter_type,
"Encounter_date_time" as encounter_date_time,
observations,
"Voided" as voided

FROM {{ source('source_gramin_26', 'encounters_gramin_26') }}