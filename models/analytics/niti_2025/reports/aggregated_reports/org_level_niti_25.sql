-- Groups machine_clean_niti_25 by stakeholder to measure geographic coverage 
-- plus active work, silt progress, and machine hours.
{{ config(
  materialized='table',
  tags=["analytics","analytics_niti_2025", "niti_2025", "niti", "analytical_models", "reports_niti_2025"]
) }}

SELECT
    m.stakeholder_responsible, 
    count(distinct m.district) as number_districts, 
    -- count(distinct m.taluka) as number_talukas, 
    -- count(distinct m.village) as number_villages, 
    count(distinct m.dam) as number_waterbodies,
    COUNT(m.workorderid) AS registered_work_orders,
    SUM(CASE WHEN m.workorder_endline_date IS NOT NULL THEN 1 ELSE 0 END) AS work_order_endlines_completed,
    SUM(CASE WHEN m.workorder_endline_date IS NULL THEN 1 ELSE 0 END) AS active_work_orders,
    SUM(CAST(m.workorder_endline_date AS DATE) - CAST(m.work_order_start_date AS DATE)) AS active_work_days,
    SUM(CASE WHEN m.workorder_endline_date IS NULL THEN COALESCE(m.total_number_of_farmers, 0) ELSE 0
        END) AS active_farmers,
    SUM(CASE WHEN m.workorder_endline_date IS NULL THEN COALESCE(m.poclain_count, 0)
    ELSE 0 END) AS active_poclains,
    SUM(CASE WHEN m.workorder_endline_date IS NULL THEN COALESCE(m.jcb_count, 0) ELSE 0
    END) AS active_jcbs,    
    sum(m.silt_to_be_excavated_as_per_plan) as silt_to_be_excavated_as_per_plan,
    sum(m.total_silt_carted_by_farmers) as total_silt_carted_by_farmers,
    sum(m.total_silt_excavated_by_gp_non_farm) as total_silt_excavated_by_gp_non_farm,
    sum(m.total_silt_excavated_by_gp_non_farm)+sum(m.total_silt_carted_by_farmers) as total_silt_excavated,
    sum(m.total_machine_working_hours) as total_machine_working_hours


FROM {{ref('machine_clean_niti_25')}} AS m
GROUP BY
    m.stakeholder_responsible

