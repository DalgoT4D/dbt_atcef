{{ config(
  materialized='table',
  tags=["analytics","analytics_niti_2025", "niti_2025", "niti", "analytical_models"]
) }}


SELECT
ws.workorder_first_name as workorder_name,
ws.updated_workorder_name as updated_workorder_name,

fs.farmer_name,

ws.state,
ws.district,
ws.village,
ws.taluka,
ws.dam,
ws.stakeholder_responsible,

w.trolleys_carted,
w.hyvas_carted,
w.silt_carted,
w.if_silt_used_non_farm_purpose,
w.other_purpose_of_carting_silt,
w.amt_silt_used_non_farm_purpose,

w.farmer_work_order_sub_id as work_order_id

FROM {{ ref('work_order_farmer_niti_25') }} AS w
LEFT JOIN {{ ref('work_order_regn_niti_25') }} AS ws
    ON w.farmer_work_order_sub_id = ws.subject_id 
LEFT JOIN {{ ref('farmer_regn_niti_25') }} AS fs
    ON w.farmer_beneficiary_id = fs.subject_id
    
WHERE ws.approval_status = 'Approved'
-- AND fs.approval_status = 'Approved'

