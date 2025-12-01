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
    s.observations ->> 'First name' AS gp_first_name,
    s.observations ->> 'Supervisor name' AS supervisor_name,
    s.observations ->> 'Silt usage plans' AS silt_usage_plans_exist,
    s.observations ->> 'Name of PoC for GP' AS gp_poc_name,
    s.observations ->> 'Supervisor contact number' AS supervisor_contact_number,
    s.observations ->> 'Any water committee active' AS water_committee_active,
    s.observations ->> 'Clarity - GP clear about RWB' AS gp_clarity_about_rwb,
    s.observations ->> 'Contact number of PoC for GP' AS gp_poc_contact_number,
    s.observations ->> 'How will you execute the silt usage plans' AS silt_usage_execution_plan,
    s.observations ->> 'Any plans to establish a committee for this WB' AS plans_to_establish_wb_committee


FROM {{ source('rwb_niti_2025', 'subjects_niti_2025') }} s
where s."Subject_type" = 'Farmer' and s."Voided" = false

