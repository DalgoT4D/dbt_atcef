-- Lists machine encounters by joining work_order_machine_graminpa_25 to machine registrations and approvals
--  without filtering by status.
{{ config(
  materialized='table',
  tags=["analytics","analytics_gramin_2025", "analytical_models", "reports_graminpa_2025", "status_graminpa_25"]
) }}

WITH machine_logs AS (
    SELECT
        w.eid,
        w.machine_work_order_sub_id,
        w.excavating_machine_id,
        CAST(w.encounter_date_time AS TIMESTAMP) AS encounter_date_time,
        w.log_book_image_machine,
        -- w.total_working_hours,
        w.working_hours
    FROM {{ ref('work_order_machine_graminpa_25') }} AS w
    WHERE w.voided != TRUE
)

SELECT
    -- ml.eid,
    ml.machine_work_order_sub_id,
    ml.excavating_machine_id AS machine_id,
    mr.machine_name,
    -- ml.total_working_hours,
    ml.encounter_date_time,
    -- mr.machine_type,
    mr.contractor_name,
    mr.contractor_mobile_number,
    mr.state,
    mr.district,
    mr.taluka,
    mr.village,
    mr.dam,
    mr.gram_panchayat_name as gp,
    mr.stakeholder_responsible,
    a.approval_status AS approval_status,
    -- a.approval_status AS encounter_approval_status,

    ml.log_book_image_machine,
    ml.working_hours

FROM machine_logs AS ml
LEFT JOIN {{ ref('approval_status_graminpa_25') }} AS a
    ON a.entity_id = ml.eid
LEFT JOIN {{ ref('machine_regn_graminpa_25') }} AS mr
    ON mr.subject_id = ml.excavating_machine_id

WHERE mr.approval_status = 'Approved' -- added because only registered machines can have a linelist
