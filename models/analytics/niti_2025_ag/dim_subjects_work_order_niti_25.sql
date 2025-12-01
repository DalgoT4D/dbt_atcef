  {{ config(
    materialized='table',
    tags=["analytics", "niti_2025", "niti", "analytics_intermediate"]
  ) }}

SELECT
    s."ID" AS subject_id,
    s."Registration_date" as registration_date,
    s."Subject_type" AS subject_type,
    s."Location_ID" AS location_id,
    s.observations ->> 'Year' AS year,
    s.observations ->> 'First name' AS workorder_first_name,
    s.observations ->> 'Video of Site' AS site_video_url,
    s.observations ->> 'Image 1 of the site' AS site_image_1_url,
    s.observations ->> 'Image 2 of the site' AS site_image_2_url,
    s.observations ->> 'updated workorder name' AS updated_workorder_name,
    CAST(s.observations ->> 'Silt to be excavated as per plan' AS NUMERIC) AS silt_to_be_excavated_as_per_plan


FROM {{ source('rwb_niti_2025', 'subjects_niti_2025') }} s
-- where s."Subject_type" = 'Work Order' and s."Voided" = false