  -- Loksahbhag subjects details for gdgs 2024
  
    {{ config(
    materialized='table',
    tags=["analytics", "analytics_gdgs_2024", "source", "source_cleaned_gdgs_2024"]
  ) }}

SELECT
    s."ID" AS subject_id,
    s."Registration_date" as registration_date,
    s."Subject_type" AS subject_type,
    s."Location_ID" AS location_id,

    s.observations ->> 'First name' AS loksahbhag_name,
    s.observations ->> 'Name of WB' AS waterbody_name,
    s.observations ->> 'Start Date' AS site_start_date,
    s.observations ->> 'End Date' AS site_end_date,
    s.observations ->> 'Village Code' AS village_code,
    s.observations ->> 'Total Silt Excavated' AS silt_excavated_ls,

    s.location ->> 'State' AS state_ls,
    s.location ->> 'District' AS district_ls,

    s.observations ->> 'Name of Taluka' AS taluka_ls,
    s.observations ->> 'Name of Village' AS village_ls,
    
    s."Voided" as voided

FROM {{ source('source_gdgsom_surveys', 'subjects_2024') }} s
where s."Subject_type" = 'Lok-sahbhag'
--  s."Voided" = false