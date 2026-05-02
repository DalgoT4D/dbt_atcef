-- Lists machine encounters by joining work_order_machine_gdgs_25 to machine registrations and approvals
--  without filtering by status.
{{ config(
  materialized='table',
  tags=["analytics","analytics_gdgs_2025", "analytical_models", "reports_gdgs_2025", "gdgs_25_approval_status"]
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
    FROM {{ ref('work_order_machine_gdgs_25') }} AS w
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
    -- a.approval_status AS encounter_approval_status,
    a.approval_status AS approval_status,

    ml.log_book_image_machine,
    ml.working_hours

FROM machine_logs AS ml
LEFT JOIN {{ ref('approval_status_gdgs_25') }} AS a
    ON a.entity_id = ml.eid
LEFT JOIN {{ ref('machine_regn_gdgs_25') }} AS mr
    ON mr.subject_id = ml.excavating_machine_id 
INNER JOIN {{ ref('work_order_regn_gdgs_25')}} AS wr
    ON ml.machine_work_order_sub_id = wr.subject_id

WHERE mr.approval_status = 'Approved' -- added because only registered machines can have a linelist
AND wr.subject_id IS NOT NULL -- added that workorder should exist for daily machine linelists to happen
