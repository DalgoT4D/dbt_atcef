  -- contains information from the encounters staging table filtered for "Excavating Machine Endline"
  
  with base as (
  {{ config(
    materialized='table',
    tags=["analytics", "analytics_gdgs_2023",  "analytics_intermediate", "analytics_encounters_gdgs_23"]
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

  FROM {{ ref('encounter_type_gdgs_23') }} e
  WHERE e.encounter_type = 'Excavating Machine Endline' 
--   and e.voided = false
  )
  
 {{ dbt_utils.deduplicate(
    relation='base',
    partition_by='eid',
    order_by='eid desc',
   )
}}
