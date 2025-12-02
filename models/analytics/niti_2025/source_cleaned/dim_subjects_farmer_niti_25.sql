  {{ config(
    materialized='table',
    tags=["analytics", "niti_2025", "niti", "analytics_intermediate", "source_cleaned_niti_2025"]
  ) }}

SELECT
    s."ID" AS subject_id,
    s."Registration_date" as registration_date,
    s."Subject_type" AS subject_type,
    s."Location_ID" AS location_id,
    s.observations ->> 'Gender' AS gender,
    s.observations ->> 'Last name' AS farmer_last_name,
    s.observations ->> 'First name' AS farmer_first_name,
    CAST(s.observations ->> 'Land holding' AS NUMERIC) AS land_holding_acres,
    s.observations ->> 'Date of birth' AS date_of_birth,
    -- Extracting nested Mobile Number fields
    s.observations -> 'Mobile Number' ->> 'phoneNumber' AS mobile_number,
    s.observations -> 'Mobile Number' ->> 'verified' AS mobile_verified_status,
    s.observations ->> 'Category of farmer' AS farmer_category,
    CAST(s.observations ->> 'Total silt required' AS NUMERIC) AS total_silt_required,
    CAST(s.observations ->> 'Number of hywas required' AS NUMERIC) AS number_hywas_required,
    CAST(s.observations ->> 'Number of trolleys required' AS NUMERIC) AS number_trolleys_required,
    CAST(s.observations ->> 'Capacity of trolleys in cu.m.' AS NUMERIC) AS capacity_trolleys_cum,
    CAST(s.observations ->> 'Farmer contribution per trolley' AS NUMERIC) AS farmer_contribution_per_trolley,
    s."Voided" as voided

FROM {{ source('rwb_niti_2025', 'subjects_niti_2025') }} s
where s."Subject_type" = 'Farmer' 
-- and s."Voided" = false
-- check voided, approval, silt target not nulls

