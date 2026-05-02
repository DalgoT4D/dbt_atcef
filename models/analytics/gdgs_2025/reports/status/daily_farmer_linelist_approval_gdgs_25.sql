-- Joins farmer registrations, work orders, and non-voided daily encounters with approval flags for review.
{{ config(
  materialized='table',
  tags=["analytics","analytics_gdgs_2025", "analytical_models", "reports_gdgs_2025", "gdgs_25_approval_status"]
) }}

WITH non_voided_work_orders AS (
    SELECT
        *
    FROM {{ ref('work_order_farmer_gdgs_25') }}
    WHERE COALESCE(voided, FALSE) = FALSE
)

SELECT
    ws.workorder_first_name AS workorder_name,
    ws.updated_workorder_name AS updated_workorder_name,
    fs.farmer_name,
    fs.subject_id as farmer_id, -- new
    fs.state,
    fs.district,
    fs.village,
    fs.taluka,
    fs.dam,
    fs.gp,
    fs.stakeholder_responsible,

    w.trolleys_carted,
    w.hyvas_carted,
    w.silt_carted,
    w.machine_sub_id AS machine_id,
    ws.silt_target,
    w.if_silt_used_non_farm_purpose,
    w.purpose_of_carting_silt,
    w.amt_silt_used_non_farm_purpose,
    w.other_person_taking_silt,
    w.other_purpose_of_carting_silt,
    w.silt_taken_by,
    w.encounter_date_time,

    w.farmer_work_order_sub_id AS work_order_id,
    ws.approval_status AS work_order_approval_status,
    fs.approval_status AS farmer_approval_status,
    a.approval_status AS approval_status
    -- a.approval_status AS encounter_approval_status


FROM {{ ref('farmer_regn_gdgs_25') }} AS fs
LEFT JOIN non_voided_work_orders AS w
    ON fs.subject_id = w.farmer_beneficiary_id
LEFT JOIN {{ ref('work_order_regn_gdgs_25') }} AS ws   -- Join on the work order subject ID
    ON w.farmer_work_order_sub_id = ws.subject_id
LEFT JOIN {{ref('approval_status_gdgs_25')}} AS a   -- Join on the encounter/event ID (eid) for approval status
    ON w.eid = a.entity_id

WHERE fs.approval_status = 'Approved' 
AND ws.approval_status = 'Approved' 
--     AND a.approval_status = 'Approved' -- this is the specific encounter approval status
