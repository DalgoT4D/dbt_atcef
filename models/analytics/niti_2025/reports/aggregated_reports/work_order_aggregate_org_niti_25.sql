{{ config(
  materialized='table',
  tags=["analytics","analytics_niti_2025", "niti_2025", "niti", "analytical_models", "reports_niti_2025"]
) }}


with active_days as (SELECT
    e.subject_id,
    COUNT(DISTINCT CAST(e.encounter_date_time AS DATE)) AS active_days_workorder
FROM
    {{ref('encounter_type_niti_25')}} e
JOIN
    {{ref('approval_status_niti_25')}} a
    ON e.eid = a.entity_id
WHERE
    a.approval_status = 'Approved'
GROUP BY
    e.subject_id)


SELECT
    aw.workorderid,
    aw.workorder_name,
    aw.updated_workorder_name,
    ad.active_days_workorder,
    aw.state,
    aw.district,
    aw.taluka,
    aw.village,
    aw.dam,
    aw.stakeholder_responsible,
    aw.silt_to_be_excavated_as_per_plan,
    aw.total_silt_carted_by_farmers,
    aw.total_silt_excavated_by_gp_non_farm,
    aw.total_silt_excavated,
    aw.total_machine_working_hours,
    aw.poclain_count,
    aw.jcb_count,
    aw.total_number_of_farmers,
    aw.work_order_start_date,
    aw.workorder_endline_date

from  {{ref('active_workorder_linelist_niti_25')}} as aw
left join active_days as ad
on aw.workorderid = ad.subject_id