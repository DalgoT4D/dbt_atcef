{{ config(
  materialized='table',
  tags=["analytics","analytics_niti_2025", "niti_2025", "niti", "analytical_models", "reports_niti_2025"]
) }}


SELECT
ws.workorder_first_name AS workorder_name,
ws.updated_workorder_name AS updated_workorder_name,
-- fs.subject_id,
fs.farmer_name,

fs.state,
fs.district,
fs.village,
fs.taluka,
fs.dam,
fs.stakeholder_responsible,

w.trolleys_carted,
w.hyvas_carted,
w.silt_carted,
w.if_silt_used_non_farm_purpose,
w.other_purpose_of_carting_silt,
w.amt_silt_used_non_farm_purpose,

w.farmer_work_order_sub_id AS work_order_id,
ws.approval_status AS work_order_approval_status,
fs.approval_status AS farmer_approval_status

FROM {{ ref('farmer_regn_niti_25') }} AS fs
LEFT JOIN {{ ref('work_order_farmer_niti_25') }} AS w
    ON fs.subject_id = w.farmer_beneficiary_id
    AND COALESCE(w.voided, FALSE) = FALSE
LEFT JOIN {{ ref('work_order_regn_niti_25') }} AS ws
    ON w.farmer_work_order_sub_id = ws.subject_id
    AND ws.approval_status = 'Approved'
WHERE fs.approval_status = 'Approved'
