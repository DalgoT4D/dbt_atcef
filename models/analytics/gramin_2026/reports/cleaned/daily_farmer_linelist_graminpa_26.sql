-- Reuses the approval linelist and keeps only records with approved farmer, work order, and encounter statuses.
{{ config(
  materialized='table',
  tags=["analytics","analytics_gramin_2026", "analytical_models", "reports_graminpa_2026", "cleaned_graminpa_26"]
) }}


-- SELECT *
-- FROM {{ ref('daily_farmer_linelist_approval_graminpa_26') }}
-- WHERE 
-- -- work_order_approval_status = 'Approved'
-- --   AND farmer_approval_status = 'Approved'
--   -- AND 
--   encounter_approval_status = 'Approved'



WITH non_voided_work_orders AS (
    SELECT
        *
    FROM {{ ref('work_order_farmer_graminpa_26') }}
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
    fs.stakeholder_responsible AS ngo_name,

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
    CAST(w.encounter_date_time AS TIMESTAMP) AS date_time, -- Source: farmer daily-recording encounter date; safe to change/remove.

    w.farmer_work_order_sub_id AS work_order_id,
    ws.approval_status AS work_order_approval_status,
    fs.approval_status AS farmer_approval_status,
    -- a.approval_status AS encounter_approval_status,
    a.approval_status AS approval_status

FROM {{ ref('farmer_regn_graminpa_26') }} AS fs
LEFT JOIN non_voided_work_orders AS w
    ON fs.subject_id = w.farmer_beneficiary_id
LEFT JOIN {{ ref('work_order_regn_graminpa_26') }} AS ws   -- Join on the work order subject ID
    ON w.farmer_work_order_sub_id = ws.subject_id
LEFT JOIN {{ref('approval_status_graminpa_26')}} AS a   -- Join on the encounter/event ID (eid) for approval status
    ON w.eid = a.entity_id

WHERE fs.approval_status = 'Approved' 
    AND ws.approval_status = 'Approved' 
    AND a.approval_status = 'Approved'
