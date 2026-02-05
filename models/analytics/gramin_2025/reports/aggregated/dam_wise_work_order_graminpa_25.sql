-- Aggregates machine_clean_graminpa_25 by dam hierarchy to split active vs completed 
-- work orders and total farmers, machines, silt, and hours.
{{ config(
  materialized='table',
  tags=["analytics","analytics_graminpa_2025", "graminpa_2025", "graminpa", "analytical_models", "reports_graminpa_2025", "aggregated_graminpa_25"]
) }}


SELECT
    m.dam,
    m.state, 
    m.district, 
    m.taluka, 
    m.village, 
    COUNT(m.workorderid) AS registered_work_orders,
    SUM(CASE WHEN m.workorder_endline_date IS NOT NULL THEN 1 ELSE 0 END) AS work_order_endlines_completed,
    SUM(CASE WHEN m.workorder_endline_date IS NULL THEN 1 ELSE 0 END) AS active_work_orders,
    sum(m.active_farmers) as active_farmers,
    sum(m.active_poclains) as active_poclains,
    sum(m.active_jcbs) as active_jcbs,
    sum(m.silt_to_be_excavated_as_per_plan) as silt_to_be_excavated_as_per_plan,
    sum(m.total_silt_carted_by_farmers) as total_silt_carted_by_farmers,
    sum(m.total_silt_excavated_by_gp_non_farm) as total_silt_excavated_by_gp_non_farm,
    sum(m.total_silt_excavated_by_gp_non_farm)+sum(m.total_silt_carted_by_farmers) as total_silt_excavated,
    ROUND(((sum(m.total_silt_excavated_by_gp_non_farm)
    +sum(m.total_silt_carted_by_farmers))*100
    / NULLIF(sum(m.silt_to_be_excavated_as_per_plan), 0)), 2) AS percent_silt_excavated, -- dam level

    SUM(CAST(m.workorder_endline_date AS DATE) - CAST(m.work_order_start_date AS DATE)) AS active_work_days,
    sum(m.total_machine_working_hours) as total_machine_working_hours,
    sum(m.jcb_working_hours) as jcb_working_hours,
    sum(m.poclain_working_hours) as poclain_working_hours,
    sum(m.jcb_excavation) as total_jcb_excavation,
    sum(m.poclain_excavation) as total_poclain_excavation


FROM {{ref('active_work_order_graminpa_25')}} AS m

GROUP BY
    m.dam,
    m.state, 
    m.district, 
    m.taluka, 
    m.village
-- SUM(CASE WHEN m.workorder_endline_date IS NULL THEN COALESCE(m.total_number_of_farmers, 0) ELSE 0
    --     END) AS active_farmers,
    -- SUM(CASE WHEN m.workorder_endline_date IS NULL THEN COALESCE(m.poclain_count, 0)
    -- ELSE 0 END) AS active_poclains,
    -- SUM(CASE WHEN m.workorder_endline_date IS NULL THEN COALESCE(m.jcb_count, 0) ELSE 0
    -- END) AS active_jcbs,    
  