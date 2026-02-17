-- contains information from the encounters staging table filtered for "Work order daily Recording - Machine"
  {{ config(
    materialized='table',
    tags=["analytics", "analytics_niti_2025", "analytics_intermediate", "encounters_niti_2025"]
  ) }}

  SELECT
      e.eid,
      e.subject_id as machine_work_order_sub_id,
      e.encounter_type,
      e.subject_type,
      e.encounter_date_time,
      e.observations ->> 'Log book image 1' AS log_book_image_machine,
      e.observations ->> 'Excavating Machine' AS excavating_machine_id,
      CAST(e.observations ->> 'Total working hours' AS NUMERIC) AS total_working_hours,
      CAST(e.observations ->> 'Working Hours as per time' AS NUMERIC) AS working_hours,
      e.voided
  FROM {{ ref('encounter_type_niti_25') }} e
  WHERE e.encounter_type = 'Work order daily Recording - Machine' 
--   and e.voided = false