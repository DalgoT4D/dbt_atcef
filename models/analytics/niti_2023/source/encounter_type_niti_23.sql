-- Raw encounter listing that keeps subject identifiers, encounter types, timestamps, and the JSON observations.
{{ config(
  materialized='table',
  tags=["analytics", "analytics_niti_2023", "analytics_intermediate", "source_cleaned_niti_2023"]
) }}


SELECT 
"ID" as eid,
"Subject_ID" as subject_id,
"Subject_type" as subject_type,
"Encounter_type" as encounter_type,
"Encounter_date_time" as encounter_date_time,
observations,
"Voided" as voided

FROM {{ source('source_atecf_surveys', 'encounter_2023') }}