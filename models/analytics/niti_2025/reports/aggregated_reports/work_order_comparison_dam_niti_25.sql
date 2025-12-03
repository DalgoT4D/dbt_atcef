{{ config(
  materialized='table',
  tags=["analytics","analytics_niti_2025", "niti_2025", "niti", "analytical_models", "reports_niti_2025"]
) }}


SELECT
dam,
COUNT(workorderid) AS registered_work_orders,
SUM(CASE WHEN workorder_endline_date IS NOT NULL THEN 1 ELSE 0 END) AS work_order_endlines_completed,
SUM(CASE WHEN workorder_endline_date IS NULL THEN 1 ELSE 0 END) AS active_work_orders,
sum(silt_to_be_excavated_as_per_plan) as silt_to_be_excavated_as_per_plan,
sum(total_silt_carted_by_farmers) as total_silt_excavated,
sum(jcb_excavation) as total_jcb_excavation,
sum(poclain_excavation) as total_poclain_excavation,
sum(jcb_working_hours) as jcb_working_hours,
sum(poclain_working_hours) as poclain_working_hours,

state,
district,
taluka,
village

from {{ref('machine_clean_niti_25')}}

group by
dam,
state,
district,
taluka,
village