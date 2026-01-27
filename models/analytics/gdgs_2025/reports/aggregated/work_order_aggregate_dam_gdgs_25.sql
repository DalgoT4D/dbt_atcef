-- Summarizes active_workorder_linelist_gdgs_25 per work order/dam with silt, farmer, machine totals and elapsed days.
{{ config(
  materialized='table',
  tags=["analytics","analytics_gdgs_2025", "gdgs_2025", "gdgs", "analytical_models", "reports_gdgs_2025", "aggregated_gdgs_25"]
) }}




SELECT
    aw.dam,
    -- aw.stakeholder_responsible,
    aw.workorderid,
    aw.workorder_name,
    aw.updated_workorder_name,
    aw.state,
    aw.district,
    aw.taluka,
    aw.village,    
    sum(aw.silt_to_be_excavated_as_per_plan) as silt_to_be_excavated_as_per_plan,
    sum(aw.total_silt_carted_by_farmers) as total_silt_carted_by_farmers,
    sum(aw.total_silt_excavated) as total_silt_excavated,
    sum(aw.total_machine_working_hours) as total_machine_working_hours,
    sum(aw.poclain_count) as poclain_count,
    sum(aw.jcb_count) as jcb_count,
    sum(aw.total_number_of_farmers) as total_number_of_farmers,
    (CAST(aw.workorder_endline_date AS DATE) - CAST(aw.work_order_start_date AS DATE)) AS active_work_days,
    aw.work_order_start_date,
    aw.workorder_endline_date

from  {{ref('active_work_order_gdgs_25')}} as aw
group by 
    aw.dam,
    aw.workorderid,
    aw.workorder_name,
    aw.state,
    aw.district,
    aw.taluka,
    aw.village,    
    aw.updated_workorder_name,
    aw.work_order_start_date,-- remove later
    aw.workorder_endline_date -- remove later

