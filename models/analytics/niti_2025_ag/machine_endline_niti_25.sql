  {{ config(
    materialized='table',
    tags=["analytics", "niti_2025", "niti", "analytics_intermediate"]
  ) }}

  SELECT
      e.eid,
      e.subject_id as endline_machine_sub_id,
      e.encounter_type,
      e.subject_type,
      e.encounter_date_time,
      e.observations ->> 'NGO Name' AS ngo_name,
      CAST(e.observations ->> 'Total working hours of machine' AS NUMERIC) AS total_machine_working_hours,
      e.voided

  FROM {{ ref('encounter_type_niti_25') }} e
  WHERE e.encounter_type = 'Excavating Machine Endline' 
--   and e.voided = false