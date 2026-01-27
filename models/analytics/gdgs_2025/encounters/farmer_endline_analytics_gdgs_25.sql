   -- contains information from the encounters staging table filtered for "Farmer Endline"
  {{ config(
    materialized='table',
    tags=["analytics", "gdgs_2025", "gdgs", "analytics_intermediate", "analytics_encounters_gdgs_25"]
  ) }}

  SELECT 
      e.eid,
      e.subject_id as endline_farmer_sub_id,
      e.encounter_type,
      e.subject_type,
      e.encounter_date_time,
      CAST(e.observations ->> 'Land holding (acres)' AS NUMERIC) AS land_holding,
      CAST(e.observations ->> 'Total silt excavated' AS NUMERIC) AS total_silt_excavated,
      e.observations ->> 'Distance from waterbody' AS distance_from_waterbody,
      e.observations ->> 'Major crops grown on the land' AS major_crops_grown,
      e.observations ->> 'Type of land silt is spread on' AS type_of_land_silt_is_spread_on,
      e.observations ->> 'Category of farmer - Farmer Endline' AS farmer_category,
      CAST(e.observations ->> 'The total farm area on which Silt is spread' AS NUMERIC) AS area_silt_spread,
      e.observations ->> 'MGNREGA card number or any other ID card number' AS mgnrega_card_number,
      CAST(e.observations ->> 'Total cost borne by the farmer for carting silt (INR)' AS NUMERIC) AS total_carting_cost,
      e.observations ->> 'Major crops to be grown on the land where silt is spread' AS major_to_be_grown,
      e.observations ->> 'Is the land-holding information correct? - Farmer Endline' AS land_holding_info_correct,
      CAST(e.observations ->> 'Total cost borne by the farmer for spreading/levelling silt on farm (INR)' AS NUMERIC) AS total_spreading_cost,
      e.voided
            
  FROM {{ ref('encounter_type_gdgs_25') }} e
  WHERE e.encounter_type = 'Farmer Endline' 
  -- and e.voided = false