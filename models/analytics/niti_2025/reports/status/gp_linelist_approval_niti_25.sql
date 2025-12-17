-- Joins gp_regn_niti_25 with the latest gp_endline_niti_25 per GP to surface readiness responses 
-- and silt totals with approval status.
{{ config(
  materialized='table',
  tags=["analytics", "niti_2025", "niti", "analytical_models", "reports_niti_2025"]
) }}


with latest_endline_per_gp AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY gp_id
            ORDER BY encounter_date_time DESC
        ) AS encounter_rank
    FROM {{ ref('gp_endline_niti_25') }} 
    WHERE voided = false
)

SELECT
gr.gp_first_name as gp_name,
gr.state,
gr.district,
gr.taluka,
gr.gp_village as village,
-- gr.stakeholder_responsible,
gr.supervisor_name,
gr.supervisor_contact_number,
gr.water_committee_active,
gr.plans_to_establish_wb_committee,
gr.gp_clarity_about_rwb,
gr.gp_poc_name,
gr.gp_poc_contact_number,
gr.silt_usage_plans_exist,
gr.silt_usage_execution_plan,
gr.approval_status,
CAST(ge.total_gp_silt_excavated_non_farm as numeric) as total_gp_silt_excavated_non_farm

from {{ ref('gp_regn_niti_25') }} as gr
left join latest_endline_per_gp ge
on gr.subject_id = ge.gp_id