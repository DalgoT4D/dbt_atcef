-- contains information from the encounters staging table filtered for "Work order endline"
   
  {{ config(
    materialized='table',
    tags=["analytics", "analytics_niti_2025", "analytics_intermediate", "encounters_niti_2025"]
  ) }}

  SELECT
      e.eid,
      e.subject_id as endline_work_order_sub_id,
      e.encounter_type,
      e.subject_type,
      e.encounter_date_time,
      e.observations ->> 'NGO Name' AS ngo,
      e.observations ->> 'Video of Site' AS site_video,
      e.observations ->> 'MB Recording done' AS mb_recording_done,
      e.observations ->> 'Image 1 of the site' AS site_image_1_url, 
      e.observations ->> 'Image 2 of the site' AS site_image_2_url,
      CAST(e.observations ->> 'Total silt excavated' AS NUMERIC) AS total_silt_excavated,
      e.observations ->> 'Document of MB recording' AS mb_document_url,
      CAST(e.observations ->> 'Silt excavated as per MB recording' AS NUMERIC) AS silt_excavated_as_per_mb,
      e.observations ->> 'Is MB recording data same as app data?' AS is_mb_data_same_as_app_data,
      e.voided

  FROM {{ ref('encounter_type_niti_25') }} e
  WHERE e.encounter_type = 'Work order endline' 
  -- and e.voided = false