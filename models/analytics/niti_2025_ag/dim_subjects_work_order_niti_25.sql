  {{ config(
    materialized='table',
    tags=["analytics", "niti_2025", "niti"]
  ) }}

SELECT
    s."ID" AS subject_id,
    s."Registration_date" as registration_date,
    s."Subject_type" AS subject_type,
    s.location ->> 'Dam' AS dam,
    s.location ->> 'State' AS state,
    s.location ->> 'Taluka' AS taluka,
    s.location ->> 'District' AS district,
    s.location ->> 'GP/Village' AS gp_village,
    -- s.location ->> 'Dam External ID' AS dam_external_id,
    -- s.location ->> 'State External ID' AS state_external_id,
    -- s.location ->> 'Taluka External ID' AS taluka_external_id,
    -- s.location ->> 'District External ID' AS district_external_id,
    -- s.location ->> 'GP/Village External ID' AS gp_village_external_id,

    s.observations ->> 'Year' AS year,
    s.observations ->> 'NGO Name' AS ngo_name,
    s.observations ->> 'First name' AS workorder_first_name,
    s.observations ->> 'Video of Site' AS site_video_url,
    s.observations ->> 'Image 1 of the site' AS site_image_1_url,
    s.observations ->> 'Image 2 of the site' AS site_image_2_url,
    s.observations ->> 'updated workorder name' AS updated_workorder_name,
    CAST(s.observations ->> 'Silt to be excavated as per plan' AS NUMERIC) AS silt_to_be_excavated_as_per_plan


FROM {{ source('rwb_niti_2025', 'subjects_niti_2025') }} s
where s."Subject_type" = 'Work Order' and s."Voided" = false