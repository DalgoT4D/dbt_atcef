{{ config(
  materialized='table',
  tags=["analytics","analytics_niti_2025", "niti_2025", "niti", "analytical_models", "reports_niti_2025", "intermediate_reports_niti_2025"]
) }}




-- Farmer silt details

SELECT
    wf.farmer_work_order_sub_id as workorderid,
    SUM(CASE
            -- Only sum the silt_carted if the record is approved
            WHEN a.approval_status = 'Approved' THEN COALESCE(wf.silt_carted, 0)
            ELSE 0
END) as total_silt_carted_by_farmers,
    count(distinct wf.farmer_beneficiary_id) as total_number_of_farmers

from {{ ref('work_order_farmer_niti_25') }} as wf

INNER JOIN {{ ref('approval_status_niti_25') }} as a 
    ON wf.eid = a.entity_id 
WHERE wf.voided != TRUE
group by wf.farmer_work_order_sub_id