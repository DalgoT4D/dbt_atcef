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

    s.observations ->> 'NGO Name' AS ngo_name,
    s.observations ->> 'First name' AS machine_name,
    s.observations ->> 'Type of Machine' AS machine_type,
    s.observations ->> 'Contractor''s name' AS contractor_name,
    CAST(s.observations ->> 'Contractor''s Mobile number' AS NUMERIC) AS contractor_mobile_number

FROM {{ source('rwb_niti_2025', 'subjects_niti_2025') }} s
where s."Subject_type" = 'Excavating Machine' and s."Voided" = false