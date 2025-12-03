{{ config(
  materialized='table',
  tags=["analytics","analytics_niti_2025", "niti_2025", "niti", "analytical_models", "reports_niti_2025"]
) }}


-- FINAL TABLE

SELECT
    wd.workorderid,
    wd.workorder_name,
    wd.updated_workorder_name,
    wd.state,
    wd.district,
    wd.taluka,
    wd.village,
    wd.dam,
    wd.stakeholder_responsible,
    wd.silt_to_be_excavated_as_per_plan,
    wd.total_silt_carted_by_farmers,
    wd.total_silt_excavated_by_gp_non_farm,
    wd.total_silt_excavated,
    wd.total_machine_working_hours,
    wd.poclain_count,
    wd.jcb_count,
    wd.total_number_of_farmers,
    wd.work_order_start_date,
    wd.workorder_endline_date
FROM {{ref('machine_clean_niti_25')}} as wd

