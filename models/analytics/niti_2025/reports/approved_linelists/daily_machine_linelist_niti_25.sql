{{ config(
  materialized='table',
  tags=["analytics","analytics_niti_2025", "niti_2025", "niti", "analytical_models", "reports_niti_2025"]
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
    FROM {{ ref('work_order_machine_niti_25') }} AS w
    WHERE w.voided != TRUE
)

SELECT
    -- ml.eid,
    ml.machine_work_order_sub_id,
    mr.machine_name,
    -- ml.excavating_machine_id,
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
    mr.stakeholder_responsible,
    a.approval_status AS encounter_approval_status,
    ml.log_book_image_machine,
    ml.working_hours

FROM machine_logs AS ml
LEFT JOIN {{ ref('approval_status_niti_25') }} AS a
    ON a.entity_id = ml.eid
LEFT JOIN {{ ref('machine_regn_niti_25') }} AS mr
    ON mr.subject_id = ml.excavating_machine_id

WHERE a.approval_status = 'Approved'
