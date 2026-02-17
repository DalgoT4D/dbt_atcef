  -- contains information from the encounters staging table filtered for "Excavating Machine Endline"
   
  {{ config(
    materialized='table',
    tags=["analytics", "analytics_gramin_2025", "analytics_intermediate", "analytics_encounters_graminpa_25"]
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

  FROM {{ ref('encounter_type_graminpa_25') }} e
  WHERE e.encounter_type = 'Excavating Machine Endline' 
--   and e.voided = false