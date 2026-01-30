-- Rolls up active_workorder_linelist_graminpa_25 by stakeholder and work order to total silt, farmer, and machine metrics along with active days.
{{ config(
  materialized='table',
  tags=["analytics","analytics_graminpa_2025", "graminpa_2025", "graminpa", "analytical_models", "reports_graminpa_2025", "aggregated_graminpa_25"]
) }}


SELECT
    aw.stakeholder_responsible,
    aw.workorderid,
    aw.workorder_name,
    aw.updated_workorder_name,
    sum(aw.silt_to_be_excavated_as_per_plan) as silt_to_be_excavated_as_per_plan,
    sum(aw.total_silt_carted_by_farmers) as total_silt_carted_by_farmers,
    sum(aw.total_silt_excavated_by_gp_non_farm) as total_silt_excavated_by_gp_non_farm,
    sum(aw.total_silt_excavated) as total_silt_excavated,
    sum(aw.total_machine_working_hours) as total_machine_working_hours,
    sum(aw.poclain_count) as poclain_count,
    sum(aw.jcb_count) as jcb_count,
    sum(aw.total_number_of_farmers) as total_number_of_farmers,
    (CAST(aw.workorder_endline_date AS DATE) - CAST(aw.work_order_start_date AS DATE)) AS active_work_days

from  {{ref('active_work_order_graminpa_25')}} as aw
group by 
    aw.stakeholder_responsible,
    aw.workorderid,
    aw.workorder_name,
    aw.updated_workorder_name,
    aw.work_order_start_date,-- remove later
    aw.workorder_endline_date -- remove later