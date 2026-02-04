-- Gram Panchayat subject slice that enriches registrations 
-- with normalized location hierarchy and qualitative readiness responses captured in observations.
  {{ config(
    materialized='table',
    tags=["analytics", "graminpa_2025", "graminpa", "analytics_intermediate", "source_cleaned_graminpa_2025"]
  ) }}

SELECT
    s."ID" AS subject_id,
    s."Registration_date" as registration_date,
    s."Subject_type" AS subject_type,
    -- s."Location_ID" AS location_id,
    s.location ->> 'State' AS state,
    s.location ->> 'Taluka' AS taluka,
    s.location ->> 'District' AS district,
    s.location ->> 'GP/Village' AS gp_village,    
    -- s.observations ->> 'NGO Name' AS stakeholder_responsible,
    s.observations ->> 'First name' AS gp_first_name,
    s.observations ->> 'Supervisor name' AS supervisor_name,
    s.observations ->> 'Silt usage plans' AS silt_usage_plans_exist,
    s.observations ->> 'Name of PoC for GP' AS gp_poc_name,
    s.observations ->> 'Supervisor contact number' AS supervisor_contact_number,
    s.observations ->> 'Any water committee active' AS water_committee_active,
    s.observations ->> 'Clarity - GP clear about RWB' AS gp_clarity_about_rwb,
    s.observations ->> 'Contact number of PoC for GP' AS gp_poc_contact_number,
    s.observations ->> 'How will you execute the silt usage plans' AS silt_usage_execution_plan,
    s.observations ->> 'Any plans to establish a committee for this WB' AS plans_to_establish_wb_committee,
    s."Voided" as voided


FROM {{ source('source_gramin_25', 'subjects_gramin_25') }} s
where s."Subject_type" = 'Gram panchayat'
--  and s."Voided" = false

