-- Work order progress status for NITI 2025 using only analytics lineage.
{{ config(
  materialized='table',
  tags=["analytics", "analytics_niti_2025", "analytical_models", "reports_niti_2025", "ss_niti_2025"]
) }}

WITH approved_work_orders AS (
    SELECT
        w.subject_id AS work_order_id,
        w.dam,
        w.state,
        w.village,
        w.district,
        w.taluka,
        w.stakeholder_responsible AS ngo_name
    FROM {{ ref('work_order_regn_niti_25') }} AS w
    WHERE w.approval_status = 'Approved'
),

-- approved_gp_endline_workorders AS (
--     SELECT DISTINCT
--         ge.endline_gp_sub_id AS work_order_id
--     FROM {{ ref('gp_endline_niti_25') }} AS ge
--     INNER JOIN {{ ref('approval_status_niti_25') }} AS a
--         ON ge.eid = a.entity_id
--     WHERE
--         ge.voided != TRUE
--         AND a.approval_status = 'Approved'
-- ),

latest_workorder_endline AS (
    SELECT
        we.workorder_id AS work_order_id,
        MAX(CAST(we.endline_date AS TIMESTAMP)) AS endline_date
    FROM {{ ref('workorder_endline_linelist_niti_25') }} AS we
    GROUP BY we.workorder_id
),

latest_farmer_activity AS (
    SELECT
        d.work_order_id,
        MAX(CAST(d.encounter_date_time AS TIMESTAMP)) AS farmer_date
    FROM {{ ref('daily_farmer_linelist_niti_25') }} AS d
    GROUP BY d.work_order_id
)

SELECT
    w.dam,
    w.work_order_id,
    w.state,
    w.village,
    w.district,
    w.taluka,
    w.ngo_name,
    e.endline_date,
    f.farmer_date,
    CASE
        WHEN e.endline_date IS NOT NULL THEN 'Completed'
        WHEN f.farmer_date IS NOT NULL THEN 'Ongoing'
        ELSE 'Not Started'
    END AS project_status,
    CASE
        WHEN e.endline_date IS NOT NULL THEN 'Endline Done'
        ELSE 'Endline Not Done'
    END AS work_order_endline_status
FROM approved_work_orders AS w
LEFT JOIN latest_workorder_endline AS e
    ON w.work_order_id = e.work_order_id
LEFT JOIN latest_farmer_activity AS f
    ON w.work_order_id = f.work_order_id
-- LEFT JOIN approved_gp_endline_workorders AS gp
--     ON w.work_order_id = gp.work_order_id
-- WHERE NOT (
--     gp.work_order_id IS NOT NULL
--     AND f.farmer_date IS NULL
-- )
