-- Filters work_order_regn_niti_25 to approved registrations and keeps key location plus start date fields per work order.
{{ config(
  materialized='table',
  tags=["analytics","analytics_niti_2025", "niti_2025", "niti", "analytical_models", "reports_niti_2025", "intermediate_reports_niti_2025"]
) }}


-- Workorder details
SELECT
w.subject_id as workorderid,
w.silt_to_be_excavated_as_per_plan,
w.state,
w.district,
w.taluka,
w.village,
w.dam,
w.stakeholder_responsible,
w.workorder_first_name as workorder_name,
w.updated_workorder_name,
cast(w.registration_date as TIMESTAMP) as work_order_start_date
from {{ ref('work_order_regn_niti_25') }} as w
WHERE w.approval_status = 'Approved'
