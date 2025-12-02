{{ config(
  materialized='table',
  tags=["analytics", "niti_2025", "niti", "analytics_intermediate", "source_cleaned_niti_2025"]
) }}


SELECT 
"ID" as eid,
"Subject_ID" as subject_id,
"Subject_type" as subject_type,
"Encounter_type" as encounter_type,
"Encounter_date_time" as encounter_date_time,
observations,
"Voided" as voided

FROM {{ source('rwb_niti_2025', 'encounters_niti_2025') }}