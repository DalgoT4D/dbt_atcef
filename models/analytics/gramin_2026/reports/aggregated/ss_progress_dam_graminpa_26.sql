-- Work order progress status for GraminPA 2026 using only analytics lineage.
{{ config(
  materialized='table',
  tags=["ss_2026", "ss_graminpa_2026"]
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
    FROM {{ ref('work_order_regn_graminpa_26') }} AS w
    WHERE w.approval_status = 'Approved'
),

latest_workorder_endline AS (
    SELECT
        we.workorder_id AS work_order_id,
        MAX(CAST(we.endline_date AS TIMESTAMP)) AS endline_date
    FROM {{ ref('workorder_endline_linelist_graminpa_26') }} AS we
    GROUP BY we.workorder_id
),

latest_farmer_activity AS (
    SELECT
        d.work_order_id,
        MAX(CAST(d.encounter_date_time AS TIMESTAMP)) AS farmer_date
    FROM {{ ref('daily_farmer_linelist_graminpa_26') }} AS d
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
