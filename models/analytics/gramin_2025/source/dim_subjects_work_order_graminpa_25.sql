  -- Work Order subjects curated with media links, year metadata,
  -- and planned silt excavation quantities for traceable project tracking.
  {{ config(
    materialized='table',
    tags=["analytics", "gramin_2025", "gramin", "source", "source_cleaned_gramin_2025"]
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
    s.observations ->> 'NOC/Work order image' AS noc_workorder_image, -- added from avni
    s.observations ->> 'Site marking image' AS site_marking_image,-- added from avni

    CAST(s.observations ->> 'Silt to be excavated as per plan' AS NUMERIC) AS silt_to_be_excavated_as_per_plan,
    s."Voided" as voided


FROM {{ source('source_gramin_25', 'subjects_gramin_25') }} s
where s."Subject_type" = 'Work Order'
--  s."Voided" = false