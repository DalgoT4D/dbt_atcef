  {{ config(
    materialized='table',
    tags=["analytics", "niti_2025", "niti", "analytics_intermediate"]
  ) }}

SELECT
    s."ID" AS subject_id,
    s."Registration_date" as registration_date,
    s."Subject_type" AS subject_type,
    s."Location_ID" AS location_id,
    -- s.observations ->> 'NGO Name' AS ngo_name,
    s.observations ->> 'First name' AS machine_name,
    s.observations ->> 'Type of Machine' AS machine_type,
    s.observations ->> 'Contractor''s name' AS contractor_name,
    CAST(s.observations ->> 'Contractor''s Mobile number' AS NUMERIC) AS contractor_mobile_number,
    s."Voided" as voided

FROM {{ source('rwb_niti_2025', 'subjects_niti_2025') }} s
where s."Subject_type" = 'Excavating Machine' 
-- and s."Voided" = false