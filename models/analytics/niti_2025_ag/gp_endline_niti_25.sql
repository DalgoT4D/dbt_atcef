  {{ config(
    materialized='table',
    tags=["analytics", "niti_2025", "niti"]
  ) }}

  SELECT
      e.eid,
      e.subject_id as endline_gp_sub_id,
      e.encounter_type,
      e.subject_type,
      e.encounter_date_time,
      
      e.observations ->> 'GP' AS gp_id,
      e.observations ->> 'Established any water committee' AS established_water_committee,
      e.observations ->> 'Capture Images for usage of silt 1' AS silt_usage_image_1_url,
      e.observations ->> 'Capture Images for usage of silt 2' AS silt_usage_image_2_url,
      
      e.observations ->> 'Usage of silt for non-farm purpose' AS gp_silt_used_non_farm,
      CAST(e.observations ->> 'Total silt excavated by GP (for non-farm purpose)' AS NUMERIC) AS total_gp_silt_excavated_non_farm,

      e.observations ->> 'Name of Point of Contact for Gram Panchayat' AS gp_poc_name,
      e.observations ->> 'Contact number of Point of Contact' AS gp_poc_contact_number,
  
      e.observations ->> 'Contact number of PoC for committee' AS committee_poc_contact_number,
      e.observations ->> 'Maintenance plan for the WB' AS has_maintenance_plan,
      e.observations ->> 'Name of PoC for the committee' AS committee_poc_name

  FROM {{ ref('encounter_type_niti_25') }} e
  WHERE e.encounter_type = 'Gram Panchayat Endline' and e.voided = false
